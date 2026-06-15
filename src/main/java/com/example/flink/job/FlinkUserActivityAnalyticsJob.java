package com.example.flink.job;

import com.example.flink.agg.PageClickCountAggregator;
import com.example.flink.agg.PageClickWindowResultFunction;
import com.example.flink.model.DlqRecord;
import com.example.flink.model.PageClickCount;
import com.example.flink.model.TopPageResult;
import com.example.flink.model.avro.UserActivityEvent;
import com.example.flink.ops.FaultInjectionMapper;
import com.example.flink.sink.OpenSearchSinkFactory;
import com.example.flink.source.AvroDeserSplitter;
import com.example.flink.source.UserActivityKafkaSourceFactory;
import com.example.flink.topn.TopNPagesFunction;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink user-activity 분석 파이프라인 Job (DAG 조립 + env 설정).
 *
 * <p>현재 구현 단계: <b>2차 전체</b> — 1차(S1~R3) 위에 Top N · DLQ · late · State TTL · 견고성을 얹었다.
 *
 * <p>현재 DAG:
 * <pre>
 *   source-user-activity-events (raw bytes)
 *     → split-deser (Avro deser, 실패 → DLQ side output)
 *         ├─(DLQ)──────────────────────────────────→ sink-opensearch-dlq
 *         └─(main) → assign-watermark (Event Time, 30s OoO, withIdleness)
 *                     → filter-click (eventType==CLICK)
 *                     → [fault-injection]  (선택: FAIL_AFTER>0일 때만)
 *                     → keyBy(pageId)
 *                     → window-pageclick-5m (5min tumbling, allowedLateness, late side output)
 *                         ├─(late)─────────────────→ sink-opensearch-late
 *                         ├─(main) ───────────────→ sink-opensearch-agg  (선택: SINK_DELAY_MS로 지연)
 *                         └─(main) → keyBy(windowEnd)
 *                                     → topn-pages (KeyedProcessFunction, 상위 N, State TTL, clear)
 *                                     → sink-opensearch-topn
 * </pre>
 *
 * <p>실행(로컬 MiniCluster + Web UI):
 * <pre>
 *   mvn -q compile exec:exec@job                 # 무한 스트리밍 → http://localhost:8082 에서 관찰
 *   BOUNDED=true mvn -q compile exec:exec@job    # 시작 시점까지만 읽고 종료(E2E 검증; UI는 즉시 닫힘)
 *   # DLQ 테스트:    mvn -q compile exec:java -Dexec.mainClass=...PoisonEventProducer
 *   # backpressure:  SINK_DELAY_MS=200 mvn -q compile exec:exec@job
 *   # 복구 데모:      FAIL_AFTER=300 mvn -q compile exec:exec@job
 * </pre>
 *
 * <p>환경변수 override: {@code BOOTSTRAP_SERVERS} / {@code SCHEMA_REGISTRY_URL} / {@code KAFKA_GROUP_ID}
 * / {@code BOUNDED} / {@code OPENSEARCH_HOST|PORT|SCHEME} / {@code WEB_UI_PORT} / {@code CHECKPOINT_DIR}
 * / {@code TOP_N} / {@code ALLOWED_LATENESS_MS} / {@code IDLENESS_MS} / {@code STATE_TTL_HOURS}
 * / {@code SINK_DELAY_MS} / {@code FAIL_AFTER}.
 */
public final class FlinkUserActivityAnalyticsJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUserActivityAnalyticsJob.class);

    private static final String DEFAULT_GROUP_ID = "flink-user-activity-analytics";

    /** W1: 로컬 MiniCluster Web UI 기본 포트. 8081은 Schema Registry가 점유하므로 8082 사용. */
    private static final int DEFAULT_WEB_UI_PORT = 8082;

    /** R1: checkpoint 주기(60초) — CLAUDE.md "Checkpoint / State 설정" 기준. */
    private static final long CHECKPOINT_INTERVAL_MS = 60_000L;

    /** R1: checkpoint 한 번이 이 시간 내에 못 끝나면 실패 처리(느린 sink로 무한정 매달림 방지). */
    private static final long CHECKPOINT_TIMEOUT_MS = 60_000L;

    /** R1: 직전 checkpoint 완료 후 다음 시작까지 최소 휴지 — 연속 checkpoint가 throughput 잠식 방지. */
    private static final long CHECKPOINT_MIN_PAUSE_MS = 30_000L;

    /** R1: 연속 checkpoint 실패를 job 실패로 간주하기 전 허용 횟수(일시적 장애 흡수). */
    private static final int TOLERABLE_CHECKPOINT_FAILURES = 3;

    /** R1: checkpoint 저장소(로컬 파일시스템) 기본 경로. retain-on-cancellation·복구 데모용. */
    private static final String DEFAULT_CHECKPOINT_DIR =
            "file:///tmp/flink-checkpoints/user-activity-analytics";

    /** 가드레일: Event Time 기준 처리, 30초 out-of-orderness 허용. */
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(30);

    /** P1: 집계 대상 이벤트 타입. */
    private static final String CLICK = "CLICK";

    /** P3: tumbling window 크기. */
    private static final org.apache.flink.streaming.api.windowing.time.Time WINDOW_SIZE =
            org.apache.flink.streaming.api.windowing.time.Time.minutes(5);

    /** 2차 Top N: 윈도우별 상위 몇 개 페이지를 낼지(기본 3). */
    private static final int DEFAULT_TOP_N = 3;

    /** 2차 late: 윈도우 종료 후 이만큼은 늦은 이벤트를 받아 윈도우를 재발화(이후는 late side output). */
    private static final long DEFAULT_ALLOWED_LATENESS_MS = 60_000L;

    /** 2차 watermark idleness: 데이터 없는 source subtask를 idle로 표시해 워터마크 정체 방지(R3 이월). */
    private static final long DEFAULT_IDLENESS_MS = 10_000L;

    /** 2차 State TTL: TopN state 누수 안전망(시간, 윈도우+lateness보다 충분히 길게). */
    private static final long DEFAULT_STATE_TTL_HOURS = 1L;

    /** 2차 late: 늦은 이벤트 side output 태그(window operator 입력 타입 = UserActivityEvent). */
    private static final OutputTag<UserActivityEvent> LATE_TAG =
            new OutputTag<>("late-events", TypeInformation.of(UserActivityEvent.class));

    private FlinkUserActivityAnalyticsJob() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = getenvOr("BOOTSTRAP_SERVERS", "localhost:9092");
        String registry = getenvOr("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        String groupId = getenvOr("KAFKA_GROUP_ID", DEFAULT_GROUP_ID);
        boolean bounded = Boolean.parseBoolean(getenvOr("BOUNDED", "false"));
        String opensearchHost = getenvOr("OPENSEARCH_HOST", "localhost");
        int opensearchPort = Integer.parseInt(getenvOr("OPENSEARCH_PORT", "9200"));
        String opensearchScheme = getenvOr("OPENSEARCH_SCHEME", "http");
        int webUiPort = Integer.parseInt(getenvOr("WEB_UI_PORT", String.valueOf(DEFAULT_WEB_UI_PORT)));
        String checkpointDir = getenvOr("CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR);

        int topN = Integer.parseInt(getenvOr("TOP_N", String.valueOf(DEFAULT_TOP_N)));
        long allowedLatenessMs =
                Long.parseLong(getenvOr("ALLOWED_LATENESS_MS", String.valueOf(DEFAULT_ALLOWED_LATENESS_MS)));
        long idlenessMs = Long.parseLong(getenvOr("IDLENESS_MS", String.valueOf(DEFAULT_IDLENESS_MS)));
        long stateTtlHours =
                Long.parseLong(getenvOr("STATE_TTL_HOURS", String.valueOf(DEFAULT_STATE_TTL_HOURS)));
        long sinkDelayMs = Long.parseLong(getenvOr("SINK_DELAY_MS", "0"));
        long failAfter = Long.parseLong(getenvOr("FAIL_AFTER", "0"));

        LOG.info("Job 설정: bootstrap={} registry={} group={} bounded={} opensearch={}://{}:{} webUiPort={} checkpointDir={}",
                bootstrap, registry, groupId, bounded, opensearchScheme, opensearchHost, opensearchPort, webUiPort, checkpointDir);
        LOG.info("2차 설정: topN={} allowedLatenessMs={} idlenessMs={} stateTtlHours={} sinkDelayMs={} failAfter={}",
                topN, allowedLatenessMs, idlenessMs, stateTtlHours, sinkDelayMs, failAfter);

        // W1 [Web UI]: 로컬 MiniCluster에 REST/Web UI를 부착해 DAG·backpressure·watermark를 관찰.
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, webUiPort);
        // 2차 견고성 — restart strategy: task/TM 장애 시 fixed-delay로 재시작 → 마지막 checkpoint에서
        //   Kafka offset + window/Top N state가 함께 복원된다(장애 복구 데모의 전제).
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // R1 [Runtime: Checkpoint] — 60초 주기 checkpoint + Kafka offset checkpoint 연동.
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        checkpointConfig.setMinPauseBetweenCheckpoints(CHECKPOINT_MIN_PAUSE_MS);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setTolerableCheckpointFailureNumber(TOLERABLE_CHECKPOINT_FAILURES);
        checkpointConfig.setExternalizedCheckpointCleanup(
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage(checkpointDir);

        // ── Source: 원본 byte[] (Avro deser는 DLQ를 위해 split-deser로 이동) ──
        KafkaSource<byte[]> source =
                UserActivityKafkaSourceFactory.create(bootstrap, registry, groupId, bounded);
        DataStreamSource<byte[]> rawEvents = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "source-user-activity-events");
        rawEvents.uid("source-user-activity-events");

        // ── 2차 DLQ [split-deser]: Avro 역직렬화(try/catch). 성공=main, 실패=DLQ side output ──
        SingleOutputStreamOperator<UserActivityEvent> events = rawEvents
                .process(new AvroDeserSplitter(registry))
                .name("split-deser").uid("split-deser");

        DataStream<DlqRecord> dlq = events.getSideOutput(AvroDeserSplitter.DLQ_TAG);
        dlq.sinkTo(OpenSearchSinkFactory.dlqSink(opensearchHost, opensearchPort, opensearchScheme))
                .name("sink-opensearch-dlq").uid("sink-opensearch-dlq");

        // S5 [assign-watermark]: Event Time = eventTime, 30s OoO + withIdleness(idle subtask 정체 방지).
        SingleOutputStreamOperator<UserActivityEvent> timestamped = events
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserActivityEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTime())
                                .withIdleness(Duration.ofMillis(idlenessMs)))
                .name("assign-watermark").uid("assign-watermark");

        // P1 [filter-click]: eventType == CLICK 만 통과.
        SingleOutputStreamOperator<UserActivityEvent> clicks = timestamped
                .filter(event -> CLICK.equals(event.getEventType()))
                .name("filter-click").uid("filter-click");

        // 2차 견고성(선택) [fault-injection]: FAIL_AFTER>0이면 한 번 일부러 죽여 checkpoint 복구를 관찰.
        DataStream<UserActivityEvent> beforeWindow = clicks;
        if (failAfter > 0) {
            beforeWindow = clicks.map(new FaultInjectionMapper(failAfter))
                    .name("fault-injection").uid("fault-injection");
        }

        // P2 keyBy(pageId) + P3 [window-pageclick-5m] + 2차 late(allowedLateness + late side output).
        SingleOutputStreamOperator<PageClickCount> pageClickCounts = beforeWindow
                .keyBy(UserActivityEvent::getPageId)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                // allowedLateness는 windowing Time, State TTL은 common Time — 타입이 달라 windowing 쪽만 풀네임.
                .allowedLateness(
                        org.apache.flink.streaming.api.windowing.time.Time.milliseconds(allowedLatenessMs))
                .sideOutputLateData(LATE_TAG)
                .aggregate(new PageClickCountAggregator(), new PageClickWindowResultFunction())
                .name("window-pageclick-5m").uid("window-pageclick-5m");

        // 2차 late [sink-opensearch-late]: allowedLateness 초과로 버려질 늦은 이벤트 → user-activity-late.
        pageClickCounts.getSideOutput(LATE_TAG)
                .sinkTo(OpenSearchSinkFactory.lateSink(opensearchHost, opensearchPort, opensearchScheme))
                .name("sink-opensearch-late").uid("sink-opensearch-late");

        // K1·K2 [sink-opensearch-agg]: PageClickCount → user-activity-agg (SINK_DELAY_MS로 지연 주입 가능).
        pageClickCounts
                .sinkTo(OpenSearchSinkFactory.aggSink(
                        opensearchHost, opensearchPort, opensearchScheme, sinkDelayMs))
                .name("sink-opensearch-agg").uid("sink-opensearch-agg");

        // 2차 Top N [topn-pages]: keyBy(windowEnd) → 윈도우별 상위 N(State TTL, 계산 후 clear).
        SingleOutputStreamOperator<TopPageResult> topNResults = pageClickCounts
                .keyBy(PageClickCount::getWindowEnd)
                .process(new TopNPagesFunction(topN, Time.hours(stateTtlHours)))
                .name("topn-pages").uid("topn-pages");

        topNResults
                .sinkTo(OpenSearchSinkFactory.topnSink(opensearchHost, opensearchPort, opensearchScheme))
                .name("sink-opensearch-topn").uid("sink-opensearch-topn");

        LOG.info("DAG 조립 완료(2차): source → split-deser(→DLQ) → assign-watermark → filter-click "
                + "→ window(→late) → {{agg, topn}} | Web UI: http://localhost:{}", webUiPort);
        LOG.info("Job 제출: 'flink-user-activity-analytics' (bounded={})", bounded);

        env.execute("flink-user-activity-analytics");
    }

    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
