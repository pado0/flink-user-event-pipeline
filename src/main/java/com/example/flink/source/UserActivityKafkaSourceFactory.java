package com.example.flink.source;

import com.example.flink.model.avro.UserActivityEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S4 — Kafka {@code user-activity-events} 토픽을 Confluent Schema Registry 기반 Avro
 * SpecificRecord({@link UserActivityEvent})로 역직렬화해 읽는 {@link KafkaSource} 팩토리.
 *
 * <p>가드레일(CLAUDE.md):
 * <ul>
 *   <li>역직렬화는 {@link ConfluentRegistryAvroDeserializationSchema#forSpecific}로 수행
 *       (메시지에 박힌 schema id로 Registry에서 writer 스키마 조회 → SpecificRecord 디코드).</li>
 *   <li>consumer group id를 명시한다(오프셋은 이후 Flink checkpoint와 함께 관리).</li>
 * </ul>
 */
public final class UserActivityKafkaSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(UserActivityKafkaSourceFactory.class);

    public static final String TOPIC = "user-activity-events";

    private UserActivityKafkaSourceFactory() {
    }

    /**
     * @param bootstrapServers  Kafka bootstrap (예: {@code localhost:9092})
     * @param schemaRegistryUrl Schema Registry URL (예: {@code http://localhost:8081})
     * @param groupId           consumer group id
     * @param bounded           true면 시작 시점 latest offset까지만 읽고 종료(검증/배치용),
     *                          false면 무한 스트리밍(기본 — 실제 파이프라인)
     */
    public static KafkaSource<UserActivityEvent> create(
            String bootstrapServers, String schemaRegistryUrl, String groupId, boolean bounded) {

        LOG.info("KafkaSource 생성: topic={} bootstrap={} registry={} group={} startingOffsets=earliest bounded={}",
                TOPIC, bootstrapServers, schemaRegistryUrl, groupId, bounded);

        KafkaSourceBuilder<UserActivityEvent> builder = KafkaSource.<UserActivityEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(TOPIC)
                .setGroupId(groupId)
                // 시작 오프셋은 earliest. (checkpoint 도입 전이라 재실행 시 처음부터 다시 읽음)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                UserActivityEvent.class, schemaRegistryUrl));

        if (bounded) {
            // S4 검증용: 시작 시점의 latest offset에 도달하면 source가 종료 → job 정상 종료.
            builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.build();
    }
}