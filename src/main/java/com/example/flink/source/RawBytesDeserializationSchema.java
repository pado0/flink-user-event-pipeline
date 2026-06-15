package com.example.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * 2차 DLQ 지원 — Kafka 메시지 value를 <b>해석하지 않고 원본 byte[] 그대로</b> 내보내는
 * {@link DeserializationSchema}.
 *
 * <p>왜 source에서 Avro를 풀지 않는가? {@code DeserializationSchema}는 side output을 낼 수 없어
 * 역직렬화 실패를 DLQ로 보낼 수 없다(가드레일 #9). 그래서 source는 bytes만 읽고, 실제 Avro 역직렬화는
 * side output이 가능한 {@link AvroDeserSplitter}(ProcessFunction)에서 try/catch로 수행한다.
 *
 * <p>{@code forSpecific}을 source에 직접 물리던 1차 구조(S4)를, 견고성(DLQ)을 위해 한 단계 뒤로 미룬 것.
 */
public final class RawBytesDeserializationSchema implements DeserializationSchema<byte[]> {

    @Override
    public byte[] deserialize(byte[] message) {
        // value 그대로 통과. (Kafka tombstone 등 null value면 null → Flink가 드롭)
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}