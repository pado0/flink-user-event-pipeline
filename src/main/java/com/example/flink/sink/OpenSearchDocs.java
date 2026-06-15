package com.example.flink.sink;

import com.example.flink.model.DlqRecord;
import com.example.flink.model.PageClickCount;
import com.example.flink.model.TopPageResult;
import com.example.flink.model.avro.UserActivityEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * K1 / 2차 — 결과 POJO를 OpenSearch 인덱스 문서(JSON)로 매핑한다. 가드레일 #3:
 * <b>입력은 Avro, sink 문서는 JSON</b>(Jackson은 sink 직렬화 전용).
 *
 * <p>핵심은 <b>deterministic document ID</b>다. OpenSearch는 트랜잭션 sink가 아니므로(가드레일 #5)
 * exactly-once에 의존하지 않고, 같은 결과는 <b>항상 같은 ID</b>로 써서 재처리 시 덮어쓰기(upsert)되도록
 * 한다 → 중복 문서 미발생(K3 멱등성). CLAUDE.md "OpenSearch 인덱스 & 문서 ID" 표를 따른다.
 *
 * <ul>
 *   <li>agg ({@link PageClickCount}): id = {@code pageId_windowStart_windowEnd_CLICK}</li>
 *   <li>topn ({@link TopPageResult}): id = {@code windowStart_windowEnd_rank_pageId}</li>
 *   <li>dlq ({@link DlqRecord}): id = 원본 바이트 SHA-256 (같은 독성 메시지는 같은 문서로 멱등)</li>
 *   <li>late ({@link UserActivityEvent}): id = eventId (이벤트 고유)</li>
 * </ul>
 */
public final class OpenSearchDocs {

    /** K2 sink 대상 인덱스 (CLAUDE.md "Window 집계" → user-activity-agg). */
    public static final String AGG_INDEX = "user-activity-agg";

    /** 2차 Top N sink 대상 인덱스. */
    public static final String TOPN_INDEX = "user-activity-topn";

    /** 2차 DLQ sink 대상 인덱스(역직렬화 실패 메시지). */
    public static final String DLQ_INDEX = "user-activity-dlq";

    /** 2차 late event sink 대상 인덱스(allowedLateness 초과로 버려질 늦은 이벤트). */
    public static final String LATE_INDEX = "user-activity-late";

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
        return toJsonBytes(doc, c);
    }

    // ── 2차 Top N (user-activity-topn) ──────────────────────────────────────────

    /**
     * deterministic 문서 ID = {@code windowStart_windowEnd_rank_pageId}.
     * rank/pageId가 tie-break로 결정적이라(같은 입력 → 같은 순위) 재처리에도 덮어쓰기로 멱등.
     */
    public static String topnDocId(TopPageResult t) {
        return String.join(
                "_",
                Long.toString(t.getWindowStart()),
                Long.toString(t.getWindowEnd()),
                Integer.toString(t.getRank()),
                t.getPageId());
    }

    /** Top N 결과 → OpenSearch 문서(JSON 바이트). */
    public static byte[] topnDocJson(TopPageResult t) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("windowStart", t.getWindowStart());
        doc.put("windowEnd", t.getWindowEnd());
        doc.put("windowStartIso", Instant.ofEpochMilli(t.getWindowStart()).toString());
        doc.put("windowEndIso", Instant.ofEpochMilli(t.getWindowEnd()).toString());
        doc.put("rank", t.getRank());
        doc.put("pageId", t.getPageId());
        doc.put("clickCount", t.getClickCount());
        return toJsonBytes(doc, t);
    }

    // ── 2차 DLQ (user-activity-dlq) ─────────────────────────────────────────────

    /**
     * deterministic 문서 ID = 원본 value 바이트의 SHA-256(hex). 같은 독성 메시지를 (earliest 재소비로)
     * 다시 읽어도 같은 문서로 덮어써 DLQ 인덱스가 중복되지 않는다.
     */
    public static String dlqDocId(DlqRecord r) {
        byte[] raw = r.getRawValue() == null ? new byte[0] : r.getRawValue();
        return sha256Hex(raw);
    }

    /** 역직렬화 실패 메시지 → OpenSearch 문서(JSON 바이트). 원본은 base64로 보존. */
    public static byte[] dlqDocJson(DlqRecord r) {
        byte[] raw = r.getRawValue() == null ? new byte[0] : r.getRawValue();
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("error", r.getError());
        doc.put("length", r.getLength());
        doc.put("rawBase64", Base64.getEncoder().encodeToString(raw));
        return toJsonBytes(doc, r);
    }

    // ── 2차 late event (user-activity-late) ─────────────────────────────────────

    /** deterministic 문서 ID = eventId(이벤트 고유). 같은 늦은 이벤트는 같은 문서로 멱등. */
    public static String lateDocId(UserActivityEvent e) {
        return e.getEventId();
    }

    /** allowedLateness 초과로 버려진 늦은 이벤트 → OpenSearch 문서(JSON 바이트). */
    public static byte[] lateDocJson(UserActivityEvent e) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("eventId", e.getEventId());
        doc.put("userId", e.getUserId());
        doc.put("pageId", e.getPageId());
        doc.put("eventType", e.getEventType());
        doc.put("eventTime", e.getEventTime());
        doc.put("eventTimeIso", Instant.ofEpochMilli(e.getEventTime()).toString());
        doc.put("sessionId", e.getSessionId());
        doc.put("reason", "late-beyond-allowed-lateness");
        return toJsonBytes(doc, e);
    }

    // ── 공통 헬퍼 ───────────────────────────────────────────────────────────────

    /** 제어된 Map 직렬화. emit이 checked 예외를 못 던지므로 직렬화 오류는 unchecked로 변환. */
    private static byte[] toJsonBytes(Map<String, Object> doc, Object source) {
        try {
            return MAPPER.writeValueAsBytes(doc);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("OpenSearch 문서 JSON 직렬화 실패: " + source, e);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256은 모든 JVM이 보장 → 도달 불가. 최후에 길이 기반 fallback.
            return "len-" + bytes.length;
        }
    }
}
