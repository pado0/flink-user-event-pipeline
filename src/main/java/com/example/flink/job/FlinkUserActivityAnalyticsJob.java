package com.example.flink.job;

import com.example.flink.model.avro.UserActivityEvent;
import com.example.flink.source.UserActivityKafkaSourceFactory;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink user-activity 분석 파이프라인 Job (DAG 조립 + env 설정).
 *
 * <p>현재 구현 단계: <b>S5</b> — KafkaSource로 읽은 Avro 이벤트에 Event Time / Watermark를 부여.
 * Filter(P1) / keyBy(P2) / Window(P3) / Sink(K2) 등은 이후 스텝에서 이 DAG에 얹는다.
 *
 * <p>현재 DAG:
 * <pre>
 *   source-user-activity-events  → assign-watermark  → watermark-probe → print-events
 *   (Kafka + Avro deser)           (Event Time, 30s     (검증용: 부여된     (콘솔 출력)
 *                                   out-of-orderness)    timestamp/watermark)
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
        //   CLAUDE.md DAG가 assign-watermark를 별도 노드로 두므로 source 뒤 operator로 분리한다.
        SingleOutputStreamOperator<UserActivityEvent> timestamped = events
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserActivityEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTime()))
                .name("assign-watermark").uid("assign-watermark");

        // S5 검증용 probe: 레코드에 부여된 event-time timestamp와 현재 watermark를 함께 출력.
        //   (이후 P3 window/타이머가 붙으면 이 probe 대신 window 결과로 검증한다.)
        timestamped
                .process(new WatermarkProbe())
                .name("watermark-probe").uid("watermark-probe")
                .print()
                .name("print-events").uid("print-events");

        env.execute("flink-user-activity-analytics");
    }

    /**
     * 각 레코드에 대해 (assign된 event-time timestamp, 현재 watermark, 핵심 필드)를 한 줄로 출력하는
     * 검증용 ProcessFunction. 상태를 두지 않는 순수 pass-through 진단기다.
     *
     * <p>참고: 일반(non-keyed) ProcessFunction이라 event-time 타이머는 등록할 수 없다(keyBy 필요 — P2).
     * bounded/버스트 입력에서는 주기적 watermark(기본 200ms) 방출 전에 레코드가 먼저 흘러가
     * 초기 레코드의 watermark가 {@code MIN}으로 보일 수 있다(정상). 부여된 timestamp가
     * eventTime과 일치하는지가 S5의 핵심 확인 포인트다.
     */
    private static final class WatermarkProbe extends ProcessFunction<UserActivityEvent, String> {

        @Override
        public void processElement(UserActivityEvent event, Context ctx, Collector<String> out) {
            long ts = ctx.timestamp();
            long wm = ctx.timerService().currentWatermark();
            out.collect(String.format(
                    "[probe] pageId=%s eventType=%s | eventTime=%d (%s) | recordTs=%d | watermark=%s",
                    event.getPageId(),
                    event.getEventType(),
                    event.getEventTime(),
                    Instant.ofEpochMilli(event.getEventTime()),
                    ts,
                    formatWatermark(wm)));
        }

        private static String formatWatermark(long wm) {
            return wm == Long.MIN_VALUE ? "MIN(uninitialized)" : wm + " (" + Instant.ofEpochMilli(wm) + ")";
        }
    }

    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}