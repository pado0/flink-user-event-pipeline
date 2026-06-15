package com.example.flink.producer;

import com.example.flink.model.avro.UserActivityEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * S3 — 샘플 {@link UserActivityEvent}(Avro / SpecificRecord)를 Kafka {@code user-activity-events}
 * 토픽에 적재한다. 학습용 데이터 생성기.
 *
 * <p>Confluent {@link KafkaAvroSerializer}가 첫 직렬화 시 스키마를 Schema Registry의 subject
 * {@code user-activity-events-value}(TopicNameStrategy)로 자동 등록한다 → S3 검증 게이트 충족.
 *
 * <p>입력은 Avro이지만, 이 클래스는 Flink DAG와 무관한 외부 producer 유틸리티다
 * (CLAUDE.md 가드레일의 "입력 Avro / 출력 JSON"에서 입력 쪽을 만들어 주는 도구).
 *
 * <p>실행:
 * <pre>
 *   mvn -q compile exec:java                  # 기본 60건
 *   mvn -q compile exec:java -Dexec.args=200  # 200건
 * </pre>
 * 엔드포인트는 환경변수 {@code BOOTSTRAP_SERVERS} / {@code SCHEMA_REGISTRY_URL}로 override 가능.
 */
public final class SampleEventProducer {

    private static final Logger LOG = LoggerFactory.getLogger(SampleEventProducer.class);

    private static final String TOPIC = "user-activity-events";

    private static final List<String> PAGE_IDS =
            List.of("page-home", "page-search", "page-product", "page-cart", "page-checkout");
    private static final List<String> USER_IDS =
            List.of("user-1", "user-2", "user-3", "user-4", "user-5");
    // CLICK 비중을 높여, 이후 filter(eventType==CLICK) 뒤에도 충분한 데이터가 남도록 함.
    private static final List<String> EVENT_TYPES =
            List.of("CLICK", "CLICK", "CLICK", "VIEW");

    private SampleEventProducer() {
    }

    public static void main(String[] args) {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 60;
        String bootstrap = getenvOr("BOOTSTRAP_SERVERS", "localhost:9092");
        String registry = getenvOr("SCHEMA_REGISTRY_URL", "http://localhost:8081");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-event-producer");

        LOG.info("producer 시작: bootstrap={} registry={} topic={} count={}",
                bootstrap, registry, TOPIC, count);

        long now = System.currentTimeMillis();
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();

        try (Producer<String, UserActivityEvent> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                UserActivityEvent event = randomEvent(now);
                // key=pageId → 동일 pageId는 동일 파티션. 이후 keyBy(pageId) 전처리와 일관성 학습용.
                ProducerRecord<String, UserActivityEvent> record =
                        new ProducerRecord<>(TOPIC, event.getPageId(), event);
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        fail.incrementAndGet();
                        LOG.error("producer send 실패", ex);
                    } else {
                        ok.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }

        LOG.info("producer 완료: sent={} failed={}", ok.get(), fail.get());
        if (fail.get() > 0) {
            System.exit(1);
        }
    }

    private static UserActivityEvent randomEvent(long baseEpochMs) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        // eventTime을 현재 기준 과거 0~2분 범위로 흩뿌림 → 이후 watermark / window 학습용.
        long eventTime = baseEpochMs - r.nextLong(0, 120_000);
        String userId = USER_IDS.get(r.nextInt(USER_IDS.size()));
        return UserActivityEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setUserId(userId)
                .setPageId(PAGE_IDS.get(r.nextInt(PAGE_IDS.size())))
                .setEventType(EVENT_TYPES.get(r.nextInt(EVENT_TYPES.size())))
                .setEventTime(eventTime)
                // sessionId는 nullable(["null","string"]) — 절반 정도만 채워 null 케이스도 흘려본다.
                .setSessionId(r.nextBoolean() ? "sess-" + userId : null)
                .build();
    }

    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
