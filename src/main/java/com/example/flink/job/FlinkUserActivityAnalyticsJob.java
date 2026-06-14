package com.example.flink.job;

import com.example.flink.model.avro.UserActivityEvent;
import com.example.flink.source.UserActivityKafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink user-activity 분석 파이프라인 Job (DAG 조립 + env 설정).
 *
 * <p>현재 구현 단계: <b>S4</b> — KafkaSource로 Avro 이벤트를 읽어 {@code print()}로 검증.
 * Watermark(S5) / Filter(P1) / Window(P3) / Sink(K2) 등은 이후 스텝에서 이 DAG에 얹는다.
 *
 * <p>실행(로컬 MiniCluster):
 * <pre>
 *   mvn -q compile exec:java                 # 무한 스트리밍
 *   BOUNDED=true mvn -q compile exec:java    # 시작 시점까지만 읽고 종료(S4 검증용)
 * </pre>
 * 엔드포인트/그룹은 환경변수 {@code BOOTSTRAP_SERVERS} / {@code SCHEMA_REGISTRY_URL} /
 * {@code KAFKA_GROUP_ID} / {@code BOUNDED}로 override 가능.
 */
public final class FlinkUserActivityAnalyticsJob {

    private static final String DEFAULT_GROUP_ID = "flink-user-activity-analytics";

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

        // S4: Avro/SpecificRecord 역직렬화 후 print()로 콘솔 확인. Watermark는 S5에서 부여하므로 여기선 noWatermarks.
        DataStreamSource<UserActivityEvent> events = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "source-user-activity-events");

        events.uid("source-user-activity-events")
                .print()
                .name("print-events").uid("print-events");

        env.execute("flink-user-activity-analytics");
    }

    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}