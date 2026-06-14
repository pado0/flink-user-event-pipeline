package com.example.flink.sink;

import com.example.flink.model.PageClickCount;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch.sink.FlushBackoffType;
import org.apache.flink.connector.opensearch.sink.Opensearch2Sink;
import org.apache.flink.connector.opensearch.sink.Opensearch2SinkBuilder;
import org.apache.flink.connector.opensearch.sink.OpensearchEmitter;
import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;

/**
 * K2 — {@link PageClickCount} 스트림을 OpenSearch {@code user-activity-agg} 인덱스로 적재하는
 * {@link Opensearch2Sink} 팩토리. CLAUDE.md DAG의 {@code [sink-opensearch-agg]}.
 *
 * <p>가드레일:
 * <ul>
 *   <li><b>Bulk write + retry</b>(가드레일 #5·"OpenSearch 인덱스" 절): bulk flush(건수/크기/주기) +
 *       지수 backoff 재시도. sink 병목 시 backpressure 가능.</li>
 *   <li><b>멱등 write</b>(K3): OpenSearch는 트랜잭션 sink가 아니므로 exactly-once에 의존하지 않고
 *       {@link OpenSearchDocs#aggDocId} deterministic ID로 덮어쓰기 → 재처리 시 중복 문서 미발생.</li>
 *   <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} + deterministic ID = effectively idempotent.</li>
 * </ul>
 *
 * <p>참고: OpenSearch 2.x용 커넥터의 통합 Sink API는 {@code org.apache.flink.connector.opensearch.sink}
 * 패키지의 {@code Opensearch2*}(레거시 SinkFunction은 {@code streaming.connectors.opensearch}). 여기선
 * {@code DataStream.sinkTo(...)}에 쓰는 통합 Sink를 사용한다.
 */
public final class OpenSearchSinkFactory {

    private OpenSearchSinkFactory() {
    }

    /**
     * @param host   OpenSearch 호스트 (예: {@code localhost})
     * @param port   OpenSearch HTTP 포트 (예: {@code 9200})
     * @param scheme {@code http} 또는 {@code https}
     */
    public static Opensearch2Sink<PageClickCount> aggSink(String host, int port, String scheme) {
        // PageClickCount → IndexRequest(JSON 문서 + deterministic id). emit은 checked 예외를
        // 못 던지므로 JSON 직렬화 예외는 OpenSearchDocs 내부에서 unchecked로 변환된다.
        // emitter를 명시 타입 변수로 두어 setEmitter의 제네릭 추론(? super T)을 PageClickCount로 고정.
        OpensearchEmitter<PageClickCount> emitter = (pageClickCount, ctx, indexer) ->
                indexer.add(new IndexRequest(OpenSearchDocs.AGG_INDEX)
                        .id(OpenSearchDocs.aggDocId(pageClickCount))
                        .source(OpenSearchDocs.aggDocJson(pageClickCount), XContentType.JSON));

        return new Opensearch2SinkBuilder<PageClickCount>()
                .setHosts(new HttpHost(host, port, scheme))
                .setEmitter(emitter)
                // Bulk write: 건수/크기/주기 중 먼저 도달하는 조건에서 flush.
                .setBulkFlushMaxActions(500)
                .setBulkFlushMaxSizeMb(2)
                .setBulkFlushInterval(2000L)
                // Retry: 일시적 sink 오류(429 등)에 지수 backoff로 5회 재시도.
                .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000L)
                // at-least-once + deterministic id → 중복 입력에도 같은 문서로 덮어쓰기(멱등).
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
