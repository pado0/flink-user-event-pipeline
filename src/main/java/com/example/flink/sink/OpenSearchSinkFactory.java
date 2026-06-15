package com.example.flink.sink;

import com.example.flink.model.DlqRecord;
import com.example.flink.model.PageClickCount;
import com.example.flink.model.TopPageResult;
import com.example.flink.model.avro.UserActivityEvent;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.opensearch.sink.FlushBackoffType;
import org.apache.flink.connector.opensearch.sink.Opensearch2Sink;
import org.apache.flink.connector.opensearch.sink.Opensearch2SinkBuilder;
import org.apache.flink.connector.opensearch.sink.OpensearchEmitter;
import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * K2 / 2차 — 결과 스트림을 OpenSearch 인덱스로 적재하는 {@link Opensearch2Sink} 팩토리들.
 *
 * <p>가드레일:
 * <ul>
 *   <li><b>Bulk write + retry</b>(가드레일 #5): bulk flush(건수/크기/주기) + 지수 backoff 재시도.
 *       sink 병목 시 backpressure 가능(완료 기준의 한 항목).</li>
 *   <li><b>멱등 write</b>: OpenSearch는 트랜잭션 sink가 아니므로 exactly-once에 의존하지 않고
 *       {@link OpenSearchDocs}의 deterministic ID로 덮어쓰기 → 재처리 시 중복 문서 미발생.</li>
 *   <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} + deterministic ID = effectively idempotent.</li>
 * </ul>
 *
 * <p>네 종류의 sink(agg / topn / dlq / late)가 같은 bulk/retry 설정을 공유한다({@link #sinkWith}).
 */
public final class OpenSearchSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSinkFactory.class);

    private OpenSearchSinkFactory() {
    }

    /**
     * K2 [sink-opensearch-agg]: {@link PageClickCount} → {@code user-activity-agg}.
     *
     * @param sinkDelayMs 2차 backpressure 테스트용 — emit마다 인위적으로 지연(ms)을 줘 sink를 느리게
     *                    만든다. {@code >0}이면 bulk 큐가 차 상류로 backpressure가 전파(Web UI 관찰).
     *                    평시 {@code 0}(무영향).
     */
    public static Opensearch2Sink<PageClickCount> aggSink(
            String host, int port, String scheme, long sinkDelayMs) {
        OpensearchEmitter<PageClickCount> emitter = (pageClickCount, ctx, indexer) -> {
            maybeSleep(sinkDelayMs);
            String docId = OpenSearchDocs.aggDocId(pageClickCount);
            LOG.debug("sink-opensearch-agg index: index={} id={} count={}",
                    OpenSearchDocs.AGG_INDEX, docId, pageClickCount.getCount());
            indexer.add(new IndexRequest(OpenSearchDocs.AGG_INDEX)
                    .id(docId)
                    .source(OpenSearchDocs.aggDocJson(pageClickCount), XContentType.JSON));
        };
        return sinkWith(host, port, scheme, emitter);
    }

    /** 2차 [sink-opensearch-topn]: {@link TopPageResult} → {@code user-activity-topn}. */
    public static Opensearch2Sink<TopPageResult> topnSink(String host, int port, String scheme) {
        OpensearchEmitter<TopPageResult> emitter = (topPageResult, ctx, indexer) -> {
            String docId = OpenSearchDocs.topnDocId(topPageResult);
            LOG.debug("sink-opensearch-topn index: index={} id={} rank={} page={} count={}",
                    OpenSearchDocs.TOPN_INDEX, docId, topPageResult.getRank(),
                    topPageResult.getPageId(), topPageResult.getClickCount());
            indexer.add(new IndexRequest(OpenSearchDocs.TOPN_INDEX)
                    .id(docId)
                    .source(OpenSearchDocs.topnDocJson(topPageResult), XContentType.JSON));
        };
        return sinkWith(host, port, scheme, emitter);
    }

    /** 2차 [sink-opensearch-dlq]: {@link DlqRecord} → {@code user-activity-dlq}. */
    public static Opensearch2Sink<DlqRecord> dlqSink(String host, int port, String scheme) {
        OpensearchEmitter<DlqRecord> emitter = (dlqRecord, ctx, indexer) -> {
            String docId = OpenSearchDocs.dlqDocId(dlqRecord);
            LOG.debug("sink-opensearch-dlq index: index={} id={} len={}",
                    OpenSearchDocs.DLQ_INDEX, docId, dlqRecord.getLength());
            indexer.add(new IndexRequest(OpenSearchDocs.DLQ_INDEX)
                    .id(docId)
                    .source(OpenSearchDocs.dlqDocJson(dlqRecord), XContentType.JSON));
        };
        return sinkWith(host, port, scheme, emitter);
    }

    /** 2차 [sink-opensearch-late]: 늦은 {@link UserActivityEvent} → {@code user-activity-late}. */
    public static Opensearch2Sink<UserActivityEvent> lateSink(String host, int port, String scheme) {
        OpensearchEmitter<UserActivityEvent> emitter = (event, ctx, indexer) -> {
            String docId = OpenSearchDocs.lateDocId(event);
            LOG.debug("sink-opensearch-late index: index={} id={} page={} eventTime={}",
                    OpenSearchDocs.LATE_INDEX, docId, event.getPageId(), event.getEventTime());
            indexer.add(new IndexRequest(OpenSearchDocs.LATE_INDEX)
                    .id(docId)
                    .source(OpenSearchDocs.lateDocJson(event), XContentType.JSON));
        };
        return sinkWith(host, port, scheme, emitter);
    }

    /** agg/topn/dlq/late가 공유하는 bulk/retry/delivery 설정으로 sink를 만든다. */
    private static <T> Opensearch2Sink<T> sinkWith(
            String host, int port, String scheme, OpensearchEmitter<T> emitter) {
        return new Opensearch2SinkBuilder<T>()
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

    /** 2차 backpressure 테스트: sinkDelayMs>0이면 emit을 인위적으로 지연시켜 sink를 느리게 만든다. */
    private static void maybeSleep(long sinkDelayMs) {
        if (sinkDelayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(sinkDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
