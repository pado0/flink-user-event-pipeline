package com.example.flink.model;

import java.util.Arrays;

/**
 * 2차 DLQ — Avro 역직렬화에 실패한 "독성(poison)" Kafka 메시지의 원본 바이트와 실패 사유를 담는 POJO.
 * 이후 OpenSearch {@code user-activity-dlq} 인덱스 문서(JSON)의 원본이 된다(가드레일 #9).
 *
 * <p>{@link com.example.flink.source.AvroDeserSplitter}가 {@code forSpecific} 역직렬화를 try/catch로
 * 감싸 실패 시 파이프라인을 죽이지 않고 이 레코드를 side output으로 흘려보낸다.
 *
 * <p>의도적으로 Avro 타입을 담지 <b>않는다</b>(원본 byte[] + 에러 문자열뿐) → 깔끔한 Flink POJO로
 * 직렬화돼 side output 경로에서 Kryo-on-Avro 문제를 피한다.
 */
public class DlqRecord {

    private byte[] rawValue;   // Kafka 메시지 value 원본 바이트 (역직렬화 실패한 그대로)
    private int length;        // rawValue 길이(빠른 확인용)
    private String error;      // 실패 사유(예외 toString)

    /** Flink POJO 직렬화용 무인자 생성자. */
    public DlqRecord() {
    }

    public DlqRecord(byte[] rawValue, String error) {
        this.rawValue = rawValue;
        this.length = rawValue == null ? 0 : rawValue.length;
        this.error = error;
    }

    public byte[] getRawValue() {
        return rawValue;
    }

    public void setRawValue(byte[] rawValue) {
        this.rawValue = rawValue;
        this.length = rawValue == null ? 0 : rawValue.length;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "DlqRecord{length=" + length + ", error=" + error
                + ", rawValue=" + Arrays.toString(rawValue) + "}";
    }
}