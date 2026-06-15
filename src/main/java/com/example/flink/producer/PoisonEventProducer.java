package com.example.flink.producer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2차 DLQ 테스트용 — {@code user-activity-events} 토픽에 <b>Avro가 아닌</b> 평문 문자열 메시지를
 * 적재한다. Confluent Avro 역직렬화는 첫 바이트의 magic byte(0)와 schema id를 기대하는데, 평문은
 * 그 형식이 아니므로 {@link com.example.flink.source.AvroDeserSplitter}에서 역직렬화가 실패 →
 * side output(DLQ) → OpenSearch {@code user-activity-dlq} 인덱스로 흘러간다.
 *
 * <p>키/값 모두 {@link StringSerializer}라 정상 Avro 흐름과 한 토픽에 섞여 들어가지만, 정상 이벤트는
 * 그대로 집계되고 독성 메시지만 DLQ로 분리되는 것을 보여 준다(파이프라인은 죽지 않음 — 가드레일 #9).
 *
 * <p>실행:
 * <pre>
 *   mvn -q compile exec:java -Dexec.mainClass=com.example.flink.producer.PoisonEventProducer
 *   mvn -q compile exec:java -Dexec.mainClass=com.example.flink.producer.PoisonEventProducer -Dexec.args=10
 * </pre>
 */
public final class PoisonEventProducer {

    private static final Logger LOG = LoggerFactory.getLogger(PoisonEventProducer.class);

    private static final String TOPIC = "user-activity-events";

    private PoisonEventProducer() {
    }

    public static void main(String[] args) {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 5;
        String bootstrap = getenvOr("BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 일부러 Avro가 아닌 평문 → Confluent Avro deser 실패 유발(magic byte 불일치).
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "poison-event-producer");

        LOG.info("poison producer 시작: bootstrap={} topic={} count={} (DLQ 테스트용 비-Avro 메시지)",
                bootstrap, TOPIC, count);

        AtomicInteger ok = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String junk = "POISON-" + i + "-not-a-valid-avro-message";
                producer.send(new ProducerRecord<>(TOPIC, "poison", junk), (md, ex) -> {
                    if (ex != null) {
                        fail.incrementAndGet();
                        LOG.error("poison send 실패", ex);
                    } else {
                        ok.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }

        LOG.info("poison producer 완료: sent={} failed={}", ok.get(), fail.get());
        if (fail.get() > 0) {
            System.exit(1);
        }
    }

    private static String getenvOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
