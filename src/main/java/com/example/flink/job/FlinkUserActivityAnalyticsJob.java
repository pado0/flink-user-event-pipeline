package com.example.flink.job;

import com.example.flink.agg.PageClickCountAggregator;
import com.example.flink.agg.PageClickWindowResultFunction;
import com.example.flink.model.PageClickCount;
import com.example.flink.model.avro.UserActivityEvent;
import com.example.flink.source.UserActivityKafkaSourceFactory;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Flink user-activity 분석 파이프라인 Job (DAG 조립 + env 설정).
 *
 * <p>현재 구현 단계: <b>P3</b> — Event Time 부여한 스트림을 CLICK 필터 → pageId keyBy →
 * 5분 tumbling window로 집계해 {@link PageClickCount}를 산출한다. OpenSearch Sink(K2)는 다음 스텝.
 *
 * <p>현재 DAG:
 * <pre>
 *   source-user-activity-events  → assign-watermark → filter-click → keyBy(pageId)
 *   (Kafka + Avro deser)           (Event Time, 30s    (eventType
 *                                   out-of-orderness)   == CLICK)
 *       → window-pageclick-5m  → print-window-result
 *         (5min tumbling,        (콘솔 출력 — K2에서
 *          count → PageClickCount) OpenSearch sink로 대체 예정)
 * </pre>
 *
 * <p>실행(로컬 MiniCluster):
 * <pre>
 *   mvn -q compile exec:exec@job                 # 무한 스트리밍
 *   BOUNDED=true mvn -q compile exec:exec@job    # 시작 시점까지만 읽고 종료(검증용)
 * </pre>
 * 엔드포인트/그룹은 환경변수 {@code BOOTSTRAP_SERVERS} / {@code SCHEMA_REGISTRY_URL} /
 * {@code KAFKA_GROUP_ID} / {@code BOUNDED}로 override 가능.
 */
public final class FlinkUserActivityAnalyticsJob {

    private static final String DEFAULT_GROUP_ID = "flink-user-activity-analytics";

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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTime()))
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

        // P3 검증: 윈도우별 pageId 카운트를 콘솔 출력. (K1/K2에서 OpenSearch sink로 대체 예정)
        pageClickCounts
                .print()
                .name("print-window-result").uid("print-window-result");

        env.execute("flink-user-activity-analytics");
    }

    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
