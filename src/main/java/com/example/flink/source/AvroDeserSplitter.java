package com.example.flink.source;

import com.example.flink.model.DlqRecord;
import com.example.flink.model.avro.UserActivityEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2차 DLQ — 원본 byte[]를 Confluent Schema Registry 기반 Avro SpecificRecord
 * ({@link UserActivityEvent})로 역직렬화하되, <b>실패해도 파이프라인을 죽이지 않고</b> 실패 레코드를
 * side output(DLQ)으로 흘려보내는 {@link ProcessFunction}. CLAUDE.md 가드레일 #9의
 * "forSpecific은 실패 시 throw하므로 try/catch로 래핑"을 실제로 구현한 지점이다.
 *
 * <p>DAG 라벨 {@code [split-deser]}. main output = 정상 {@link UserActivityEvent},
 * side output {@link #DLQ_TAG} = {@link DlqRecord}(원본 bytes + 에러).
 *
 * <p>역직렬화를 source가 아니라 여기서 하는 이유는 {@link RawBytesDeserializationSchema} 참고
 * (DeserializationSchema는 side output 불가).
 */
public final class AvroDeserSplitter extends ProcessFunction<byte[], UserActivityEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserSplitter.class);

    /** Avro 역직렬화 실패 메시지가 흘러가는 side output 태그(DLQ). */
    public static final OutputTag<DlqRecord> DLQ_TAG =
            new OutputTag<>("dlq", TypeInformation.of(DlqRecord.class));

    private final String schemaRegistryUrl;

    /** Confluent registry deser. open()에서 생성/초기화(서브태스크별). transient → 직렬화 제외. */
    private transient DeserializationSchema<UserActivityEvent> inner;

    public AvroDeserSplitter(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        inner = ConfluentRegistryAvroDeserializationSchema.forSpecific(
                UserActivityEvent.class, schemaRegistryUrl);
        // RegistryAvroDeserializationSchema는 open()에서 datum reader/registry 클라이언트를 준비한다.
        // RuntimeContext를 DeserializationSchema.InitializationContext로 어댑팅해 전달.
        inner.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()));
        LOG.info("split-deser open: registry={}", schemaRegistryUrl);
    }

    @Override
    public void processElement(byte[] value, Context ctx, Collector<UserActivityEvent> out) {
        UserActivityEvent event;
        // try/catch는 역직렬화만 감싼다. out.collect()까지 감싸면 하류(window/sink)에서 난 예외를
        // "역직렬화 실패"로 오인해 정상 이벤트를 DLQ로 보내고 장애도 삼켜 버린다(restart 미발생).
        try {
            event = inner.deserialize(value);
        } catch (Exception ex) {
            // 가드레일 #9: 역직렬화 실패/스키마 불일치는 파이프라인을 죽이지 않고 DLQ로.
            LOG.warn("split-deser: Avro 역직렬화 실패 → DLQ (len={}): {}",
                    value == null ? 0 : value.length, ex.toString());
            ctx.output(DLQ_TAG, new DlqRecord(value, ex.toString()));
            return;
        }
        if (event != null) {
            // 하류 예외는 그대로 전파 → restart strategy가 checkpoint에서 복구(삼키지 않음).
            out.collect(event);
        }
    }
}