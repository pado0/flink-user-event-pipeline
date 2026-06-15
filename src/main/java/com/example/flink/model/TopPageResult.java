package com.example.flink.model;

import java.time.Instant;
import java.util.Objects;

/**
 * 2차 Top N 산출 — 한 5분 윈도우(windowStart~windowEnd) 안에서 클릭 수 상위 페이지의 순위 1건.
 * 이후 OpenSearch {@code user-activity-topn} 인덱스 문서(JSON)의 원본이 된다.
 *
 * <p>{@link com.example.flink.topn.TopNPagesFunction}이 {@code keyBy(windowEnd)} 후 윈도우별
 * 상위 N개를 계산해 rank마다 1건씩 흘려보낸다(rank 1..N). 동일 클릭수일 때 순위가 흔들리지 않도록
 * pageId 사전순으로 tie-break하므로 같은 입력이면 항상 같은 rank/pageId → deterministic doc id로 멱등.
 *
 * <p>{@link PageClickCount}와 같은 <b>Flink POJO 규칙</b>(public 무인자 생성자 + private 필드 +
 * 표준 getter/setter)을 지켜 효율적인 POJO 직렬화를 받는다.
 */
public class TopPageResult {

    private long windowStart; // epoch ms, inclusive
    private long windowEnd;   // epoch ms, exclusive
    private int rank;         // 1-based (1 = 최다 클릭)
    private String pageId;
    private long clickCount;

    /** Flink POJO 직렬화용 무인자 생성자. */
    public TopPageResult() {
    }

    public TopPageResult(long windowStart, long windowEnd, int rank, String pageId, long clickCount) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.rank = rank;
        this.pageId = pageId;
        this.clickCount = clickCount;
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

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    @Override
    public String toString() {
        return String.format(
                "TopPageResult{window=[%s ~ %s), rank=%d, pageId=%s, clickCount=%d}",
                Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowEnd), rank, pageId, clickCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TopPageResult)) {
            return false;
        }
        TopPageResult that = (TopPageResult) o;
        return windowStart == that.windowStart
                && windowEnd == that.windowEnd
                && rank == that.rank
                && clickCount == that.clickCount
                && Objects.equals(pageId, that.pageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, rank, pageId, clickCount);
    }
}