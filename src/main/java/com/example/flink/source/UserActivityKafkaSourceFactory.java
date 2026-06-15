package com.example.flink.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S4 / 2차 — Kafka {@code user-activity-events} 토픽을 읽는 {@link KafkaSource} 팩토리.
 *
 * <p><b>2차 DLQ 변경</b>: 1차에서는 source에 {@code ConfluentRegistryAvroDeserializationSchema.forSpecific}을
 * 직접 물려 Avro로 풀었지만, {@code DeserializationSchema}는 side output을 낼 수 없어 역직렬화 실패를
 * DLQ로 보낼 수 없다(가드레일 #9). 그래서 source는 {@link RawBytesDeserializationSchema}로 <b>원본 byte[]만</b>
 * 읽고, Avro 역직렬화는 side output이 가능한 {@link AvroDeserSplitter}(ProcessFunction)에서 try/catch로
 * 수행한다. forSpecific 자체는 그대로 사용하며 위치만 한 단계 뒤로 옮긴 것.
 *
 * <p>가드레일: consumer group id를 명시한다(오프셋은 Flink checkpoint와 함께 관리 — R1).
 */
public final class UserActivityKafkaSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(UserActivityKafkaSourceFactory.class);

    public static final String TOPIC = "user-activity-events";

    private UserActivityKafkaSourceFactory() {
    }

    /**
     * @param bootstrapServers  Kafka bootstrap (예: {@code localhost:9092})
     * @param schemaRegistryUrl Schema Registry URL (로깅용 — 실제 Avro deser는 AvroDeserSplitter가 사용)
     * @param groupId           consumer group id
     * @param bounded           true면 시작 시점 latest offset까지만 읽고 종료(검증/배치용),
     *                          false면 무한 스트리밍(기본 — 실제 파이프라인)
     */
    public static KafkaSource<byte[]> create(
            String bootstrapServers, String schemaRegistryUrl, String groupId, boolean bounded) {

        LOG.info("KafkaSource 생성: topic={} bootstrap={} registry={} group={} startingOffsets=earliest bounded={} (raw bytes, Avro deser는 split-deser에서)",
                TOPIC, bootstrapServers, schemaRegistryUrl, groupId, bounded);

        KafkaSourceBuilder<byte[]> builder = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(TOPIC)
                .setGroupId(groupId)
                // 시작 오프셋은 earliest (학습용: bounded 재실행마다 토픽을 처음부터 다시 읽어
                //   K3 멱등성/E2E 검증이 반복 가능). 단, checkpoint/savepoint에서 복구할 때는 이
                //   설정과 무관하게 checkpoint에 스냅샷된 offset에서 재개된다(시작 오프셋은 최초 기동만).
                .setStartingOffsets(OffsetsInitializer.earliest())
                // R1 [Kafka offset checkpoint 연동]: offset의 source of truth = Flink checkpoint.
                //   Kafka auto-commit에 의존하지 않고(KafkaSource가 내부적으로 비활성화), checkpoint
                //   완료 시점에만 모니터링/가시성 목적으로 Kafka에 offset을 commit한다(기본값 true).
                .setProperty("commit.offsets.on.checkpoint", "true")
                // 2차 DLQ: 원본 byte[]만 읽는다. Avro 역직렬화/실패 라우팅은 AvroDeserSplitter가 담당.
                .setValueOnlyDeserializer(new RawBytesDeserializationSchema());

        if (bounded) {
            // S4 검증용: 시작 시점의 latest offset에 도달하면 source가 종료 → job 정상 종료.
            builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.build();
    }
}