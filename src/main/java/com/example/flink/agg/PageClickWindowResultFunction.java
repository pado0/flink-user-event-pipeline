package com.example.flink.agg;

import com.example.flink.model.PageClickCount;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * P3 — 증분 집계된 카운트(Long)에 윈도우 메타데이터(key=pageId, windowStart/End)를 부착해
 * {@link PageClickCount}로 만드는 {@link ProcessWindowFunction}.
 *
 * <p>{@code aggregate(PageClickCountAggregator, PageClickWindowResultFunction)} 형태로 결합되어,
 * 윈도우 firing 시 누적 카운트 <b>1건</b>만 {@code Iterable}로 전달받는다(전체 이벤트가 아님 — 메모리 효율).
 *
 * <p>참고: 레거시 {@code WindowFunction}의 현대적 대체 API가 {@code ProcessWindowFunction}이다
 * ({@link Context}로 윈도우/워터마크/state 접근 가능). CLAUDE.md DAG의 {@code [window-pageclick-5m]}.
 *
 * <p>제네릭: IN=Long(집계 카운트), OUT={@link PageClickCount}, KEY=String(pageId), W={@link TimeWindow}.
 */
public final class PageClickWindowResultFunction
        extends ProcessWindowFunction<Long, PageClickCount, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(PageClickWindowResultFunction.class);

    @Override
    public void process(
            String pageId,
            Context context,
            Iterable<Long> counts,
            Collector<PageClickCount> out) {

        long count = counts.iterator().next(); // 증분 집계라 원소는 항상 1개
        TimeWindow window = context.window();
        PageClickCount result = new PageClickCount(pageId, window.getStart(), window.getEnd(), count);
        LOG.info("window-pageclick-5m fired: {}", result);
        out.collect(result);
    }
}
