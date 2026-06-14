package com.example.flink.model;

import java.time.Instant;
import java.util.Objects;

/**
 * P3 산출 — pageId별 5분 윈도우 클릭 집계 결과. 이후 OpenSearch {@code user-activity-agg}
 * 인덱스 문서(JSON)의 원본이 된다. (가드레일: 입력은 Avro, 결과 sink 문서는 JSON)
 *
 * <p>Flink가 효율적으로 직렬화하도록 <b>POJO 규칙</b>을 지킨다:
 * public 무인자 생성자 + private 필드 + 표준 getter/setter. (TypeExtractor가 POJO로 인식)
 */
public class PageClickCount {

    private String pageId;
    private long windowStart; // epoch ms, inclusive
    private long windowEnd;   // epoch ms, exclusive
    private long count;

    /** Flink POJO 직렬화용 무인자 생성자. */
    public PageClickCount() {
    }

    public PageClickCount(String pageId, long windowStart, long windowEnd, long count) {
        this.pageId = pageId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return String.format(
                "PageClickCount{pageId=%s, window=[%s ~ %s), count=%d}",
                pageId, Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowEnd), count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PageClickCount)) {
            return false;
        }
        PageClickCount that = (PageClickCount) o;
        return windowStart == that.windowStart
                && windowEnd == that.windowEnd
                && count == that.count
                && Objects.equals(pageId, that.pageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId, windowStart, windowEnd, count);
    }
}
