package com.example.flink.sink;

import com.example.flink.model.PageClickCount;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * K1 — {@link PageClickCount}(P3 산출 POJO)를 OpenSearch {@code user-activity-agg} 인덱스 문서로
 * 매핑한다. 가드레일 #3: <b>입력은 Avro, sink 문서는 JSON</b>(Jackson은 sink 직렬화 전용).
 *
 * <p>핵심은 <b>deterministic document ID</b>다. OpenSearch는 트랜잭션 sink가 아니므로(가드레일 #5)
 * exactly-once에 의존하지 않고, 같은 윈도우 결과는 <b>항상 같은 ID</b>로 써서 재처리 시 덮어쓰기(upsert)
 * 되도록 한다 → 중복 문서 미발생(K3 멱등성). CLAUDE.md "OpenSearch 인덱스 & 문서 ID" 표를 따른다.
 *
 * <p>문서 ID = {@code pageId_windowStart_windowEnd_eventType}. eventType은 filter-click(P1)을 거쳐
 * 항상 {@code CLICK}이다(집계 대상이 CLICK뿐). pageId에 {@code '_'}가 없다는 전제(현재 데이터: 5종 페이지명).
 */
public final class OpenSearchDocs {

    /** K2 sink 대상 인덱스 (CLAUDE.md "Window 집계" → user-activity-agg). */
    public static final String AGG_INDEX = "user-activity-agg";

    /** filter-click(P1) 이후 집계되는 이벤트 타입은 항상 CLICK. 문서 ID/필드에 박는다. */
    private static final String EVENT_TYPE = "CLICK";

    /** sink JSON 직렬화 전용 ObjectMapper. read/write는 thread-safe하므로 static 재사용. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OpenSearchDocs() {
    }

    /**
     * deterministic 문서 ID = {@code pageId_windowStart_windowEnd_CLICK}.
     * 동일 윈도우 결과를 여러 번 써도 같은 ID → 덮어쓰기로 멱등(K3).
     */
    public static String aggDocId(PageClickCount c) {
        return String.join(
                "_",
                c.getPageId(),
                Long.toString(c.getWindowStart()),
                Long.toString(c.getWindowEnd()),
                EVENT_TYPE);
    }

    /**
     * 집계 결과를 OpenSearch 문서(JSON 바이트)로 직렬화한다. Jackson {@code writeValueAsBytes}의
     * 결과는 그대로 {@code IndexRequest.source(byte[], XContentType.JSON)}에 들어간다.
     *
     * <p>윈도우 경계 epoch ms 외에 Dashboards에서 사람이 읽기 좋은 ISO-8601(UTC) 보조 필드를 함께 넣는다.
     * 제어된 {@link Map} 직렬화라 실패할 일이 사실상 없지만, sink {@code emit}이 checked 예외를 던질 수
     * 없으므로 직렬화 오류는 unchecked로 감싼다.
     */
    public static byte[] aggDocJson(PageClickCount c) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("pageId", c.getPageId());
        doc.put("eventType", EVENT_TYPE);
        doc.put("windowStart", c.getWindowStart());
        doc.put("windowEnd", c.getWindowEnd());
        doc.put("windowStartIso", Instant.ofEpochMilli(c.getWindowStart()).toString());
        doc.put("windowEndIso", Instant.ofEpochMilli(c.getWindowEnd()).toString());
        doc.put("count", c.getCount());
        try {
            return MAPPER.writeValueAsBytes(doc);
        } catch (JsonProcessingException e) {
            // 제어된 Map이라 실질적으로 도달 불가. emit이 checked를 못 던지므로 unchecked로 변환.
            throw new IllegalStateException("PageClickCount JSON 직렬화 실패: " + c, e);
        }
    }
}
