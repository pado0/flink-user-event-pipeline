package com.example.flink.job;

import com.example.flink.agg.PageClickCountAggregator;
import com.example.flink.agg.PageClickWindowResultFunction;
import com.example.flink.model.PageClickCount;
import com.example.flink.model.avro.UserActivityEvent;
import com.example.flink.sink.OpenSearchSinkFactory;
import com.example.flink.source.UserActivityKafkaSourceFactory;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink user-activity 분석 파이프라인 Job (DAG 조립 + env 설정).
 *
 * <p>현재 구현 단계: <b>K1·K2</b> — Event Time 부여한 스트림을 CLICK 필터 → pageId keyBy →
 * 5분 tumbling window로 집계({@link PageClickCount})한 뒤, JSON 문서(deterministic id)로 매핑해
 * OpenSearch {@code user-activity-agg} 인덱스에 bulk/retry로 적재한다.
 *
 * <p>현재 DAG:
 * <pre>
 *   source-user-activity-events  → assign-watermark → filter-click → keyBy(pageId)
 *   (Kafka + Avro deser)           (Event Time, 30s    (eventType
 *                                   out-of-orderness)   == CLICK)
 *       → window-pageclick-5m  → sink-opensearch-agg
 *         (5min tumbling,        (PageClickCount → JSON 문서 + deterministic id
 *          count → PageClickCount) → OpenSearch bulk/retry, 멱등 upsert)
 * </pre>
 *
 * <p>실행(로컬 MiniCluster + Web UI):
 * <pre>
 *   mvn -q compile exec:exec@job                 # 무한 스트리밍 → http://localhost:8082 에서 관찰
 *   BOUNDED=true mvn -q compile exec:exec@job    # 시작 시점까지만 읽고 종료(검증용; UI는 즉시 닫힘)
 * </pre>
 * (W1) env를 {@link StreamExecutionEnvironment#createLocalEnvironmentWithWebUI(Configuration)}로
 * 띄워 로컬 MiniCluster Web UI(기본 포트 8082)를 활성화한다 — DAG·operator backpressure·watermark
 * 관찰용. UI는 Job 실행 중에만 생존하므로 무한 스트리밍({@code BOUNDED=false})으로 띄워야 한다.
 *
 * <p>엔드포인트/그룹은 환경변수 {@code BOOTSTRAP_SERVERS} / {@code SCHEMA_REGISTRY_URL} /
 * {@code KAFKA_GROUP_ID} / {@code BOUNDED} / {@code OPENSEARCH_HOST} / {@code OPENSEARCH_PORT} /
 * {@code OPENSEARCH_SCHEME} / {@code WEB_UI_PORT}로 override 가능.
 */
public final class FlinkUserActivityAnalyticsJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUserActivityAnalyticsJob.class);

    private static final String DEFAULT_GROUP_ID = "flink-user-activity-analytics";

    /** W1: 로컬 MiniCluster Web UI 기본 포트. 8081은 Schema Registry가 점유하므로 8082 사용. */
    private static final int DEFAULT_WEB_UI_PORT = 8082;

    /** 가드레일: Event Time 기준 처리, 30초 out-of-orderness 허용. */
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(30);

    /** P1: 집계 대상 이벤트 타입. */
    private static final String CLICK = "CLICK";

    /** P3: tumbling window 크기. */
    private static final Time WINDOW_SIZE = Time.minutes(5);

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

        LOG.info("Job 설정: bootstrap={} registry={} group={} bounded={} opensearch={}://{}:{} webUiPort={}",
                bootstrap, registry, groupId, bounded, opensearchScheme, opensearchHost, opensearchPort, webUiPort);

        // W1 [Web UI]: 로컬 MiniCluster에 REST/Web UI를 부착해 DAG·backpressure·watermark를 관찰.
        //   createLocalEnvironmentWithWebUI는 항상 로컬 MiniCluster를 띄운다(클러스터 제출은 후순위).
        //   UI는 Job 실행 중에만 생존 → 관찰하려면 BOUNDED=false(무한 스트리밍)로 실행할 것.
        //   (BOUNDED=true는 end-of-input에서 즉시 종료돼 UI 접속 불가.)
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, webUiPort);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        KafkaSource<UserActivityEvent> source =
                UserActivityKafkaSourceFactory.create(bootstrap, registry, groupId, bounded);

        // Watermark는 아래 assign-watermark operator에서 별도로 부여하므로 source에는 noWatermarks.
        DataStreamSource<UserActivityEvent> events = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "source-user-activity-events");
        events.uid("source-user-activity-events");

        // S5 [assign-watermark]: Event Time = Avro eventTime(epoch ms), 30s out-of-orderness.
        SingleOutputStreamOperator<UserActivityEvent> timestamped = events
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserActivityEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, recordTimestamp) -> {
                                    long eventTime = event.getEventTime();
                                    LOG.debug("assign-watermark: eventId={} page={} type={} eventTime={}",
                                            event.getEventId(), event.getPageId(), event.getEventType(), eventTime);
                                    return eventTime;
                                }))
                .name("assign-watermark").uid("assign-watermark");

        // P1 [filter-click]: eventType == CLICK 만 통과 (VIEW 등은 집계 대상 아님).
        SingleOutputStreamOperator<UserActivityEvent> clicks = timestamped
                .filter(event -> CLICK.equals(event.getEventType()))
                .name("filter-click").uid("filter-click");

        // P2 keyBy(pageId) + P3 [window-pageclick-5m]:
        //   동일 pageId는 동일 task로 → 5분 event-time tumbling window 단위로,
        //   증분 AggregateFunction(count) + ProcessWindowFunction(window 메타 부착) → PageClickCount.
        SingleOutputStreamOperator<PageClickCount> pageClickCounts = clicks
                .keyBy(UserActivityEvent::getPageId)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .aggregate(new PageClickCountAggregator(), new PageClickWindowResultFunction())
                .name("window-pageclick-5m").uid("window-pageclick-5m");

        // K1·K2 [sink-opensearch-agg]: PageClickCount → JSON 문서(deterministic id) → OpenSearch.
        //   K1 매핑(OpenSearchDocs) + K2 sink(bulk/retry, at-least-once). 같은 윈도우 결과는 같은
        //   doc id로 덮어쓰기 → 재처리 시 중복 문서 미발생(K3 멱등성). P3의 print()를 대체.
        pageClickCounts
                .sinkTo(OpenSearchSinkFactory.aggSink(opensearchHost, opensearchPort, opensearchScheme))
                .name("sink-opensearch-agg").uid("sink-opensearch-agg");

        LOG.info("DAG 조립 완료: source-user-activity-events → assign-watermark → filter-click "
                + "→ keyBy(pageId) → window-pageclick-5m → sink-opensearch-agg | Web UI: http://localhost:{}",
                webUiPort);
        LOG.info("Job 제출: 'flink-user-activity-analytics' (bounded={})", bounded);

        env.execute("flink-user-activity-analytics");
    }

    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
