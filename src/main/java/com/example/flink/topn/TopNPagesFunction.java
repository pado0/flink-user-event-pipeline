package com.example.flink.topn;

import com.example.flink.model.PageClickCount;
import com.example.flink.model.TopPageResult;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2차 Top N — {@code keyBy(windowEnd)} 된 {@link PageClickCount} 스트림에서 윈도우별 클릭수 상위 N개
 * 페이지를 골라 {@link TopPageResult}(rank 1..N)로 내보내는 {@link KeyedProcessFunction}.
 * DAG 라벨 {@code [topn-pages]}.
 *
 * <p><b>동작</b>: 같은 windowEnd(키)로 들어오는 페이지별 카운트를 {@link MapState}(pageId→count)에 모으고,
 * 이벤트 타임 타이머를 {@code windowEnd + 1}에 건다. 워터마크가 윈도우 끝을 지나 타이머가 발화하면
 * 모인 카운트를 정렬해 상위 N개를 emit하고 <b>큰 state를 clear</b>한다(가드레일 #6).
 *
 * <ul>
 *   <li><b>왜 windowEnd+1 타이머?</b> 상류 window operator는 워터마크 W(≥windowEnd)를 받으면 결과
 *       레코드들을 먼저 흘리고 그 뒤 W를 전파한다. 이 operator는 레코드를 모두 처리한 뒤 W로 타이머를
 *       발화하므로, 발화 시점엔 해당 윈도우의 모든 페이지가 state에 들어와 있다(표준 Top-N 패턴).</li>
 *   <li><b>tie-break</b>: 같은 클릭수면 pageId 사전순. 입력이 같으면 항상 같은 rank/pageId →
 *       deterministic doc id로 멱등(K3 철학을 Top N에도 적용).</li>
 *   <li><b>bounded state</b>: MapState는 한 윈도우의 페이지 수만큼만 쌓이고 계산 후 clear → 윈도우가
 *       쌓일수록 무한정 커지지 않는다(가드레일 #6). 추가로 {@link StateTtlConfig} TTL을 안전망으로 둔다
 *       (타이머가 끝내 발화하지 못한 키의 누수 방지).</li>
 *   <li><b>late 재발화 무시</b>: allowedLateness로 윈도우가 윈도우 종료 후 다시 firing되면 해당 페이지
 *       1건만 다시 들어온다. 이미 Top N을 확정(emit)한 윈도우는 {@code emitted} 플래그로 재계산을
 *       건너뛴다 → Top N은 "윈도우 종료 시점 확정" 시맨틱(늦게 온 갱신은 agg 인덱스에만 반영).</li>
 * </ul>
 */
public final class TopNPagesFunction
        extends KeyedProcessFunction<Long, PageClickCount, TopPageResult> {

    private static final Logger LOG = LoggerFactory.getLogger(TopNPagesFunction.class);

    /** 5분 윈도우 크기(ms) — windowStart 복원 불가 시 fallback 계산용. */
    private static final long WINDOW_SIZE_MS = 5 * 60 * 1000L;

    private final int topN;
    private final Time stateTtl;

    /** pageId → 해당 윈도우 클릭수. 계산 후 clear. */
    private transient MapState<String, Long> pageCounts;
    /** 이 윈도우의 windowStart(결과에 부착). 계산 후 clear. */
    private transient ValueState<Long> windowStartState;
    /** 이미 Top N을 확정했는지(late 재발화 무시용). 작은 플래그라 TTL이 청소. */
    private transient ValueState<Boolean> emittedState;

    public TopNPagesFunction(int topN, Time stateTtl) {
        this.topN = topN;
        this.stateTtl = stateTtl;
    }

    @Override
    public void open(Configuration parameters) {
        // 2차 운영 — State TTL: 타이머가 발화하지 못한 채 남은 windowEnd 키의 state 누수를 막는 안전망.
        // OnCreateAndWrite: 쓰기마다 만료시각 갱신. NeverReturnExpired: 만료분은 읽히지 않게.
        StateTtlConfig ttl = StateTtlConfig.newBuilder(stateTtl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, Long> countsDesc =
                new MapStateDescriptor<>("topn-page-counts", Types.STRING, Types.LONG);
        countsDesc.enableTimeToLive(ttl);
        pageCounts = getRuntimeContext().getMapState(countsDesc);

        ValueStateDescriptor<Long> startDesc =
                new ValueStateDescriptor<>("topn-window-start", Types.LONG);
        startDesc.enableTimeToLive(ttl);
        windowStartState = getRuntimeContext().getState(startDesc);

        ValueStateDescriptor<Boolean> emittedDesc =
                new ValueStateDescriptor<>("topn-emitted", Types.BOOLEAN);
        emittedDesc.enableTimeToLive(ttl);
        emittedState = getRuntimeContext().getState(emittedDesc);
    }

    @Override
    public void processElement(PageClickCount value, Context ctx, Collector<TopPageResult> out)
            throws Exception {
        if (Boolean.TRUE.equals(emittedState.value())) {
            // 이미 윈도우 종료 시점에 Top N 확정 → late 재발화 갱신은 Top N에 반영하지 않는다.
            LOG.debug("topn-pages: 확정 후 갱신 무시 windowEnd={} page={} count={}",
                    ctx.getCurrentKey(), value.getPageId(), value.getCount());
            return;
        }
        pageCounts.put(value.getPageId(), value.getCount());
        if (windowStartState.value() == null) {
            windowStartState.update(value.getWindowStart());
        }
        // 윈도우 끝을 워터마크가 지나면 발화(레코드는 워터마크보다 먼저 도착하므로 전체가 모인다).
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TopPageResult> out)
            throws Exception {
        if (Boolean.TRUE.equals(emittedState.value())) {
            return; // 안전망(정상 흐름에선 도달 안 함)
        }
        long windowEnd = ctx.getCurrentKey();
        Long ws = windowStartState.value();
        long windowStart = ws != null ? ws : windowEnd - WINDOW_SIZE_MS;

        // 윈도우의 페이지별 카운트를 모아 (count desc, pageId asc)로 정렬 → 상위 N.
        List<Map.Entry<String, Long>> entries = new ArrayList<>();
        for (Map.Entry<String, Long> e : pageCounts.entries()) {
            entries.add(Map.entry(e.getKey(), e.getValue()));
        }
        entries.sort(Comparator
                .comparingLong((Map.Entry<String, Long> e) -> e.getValue()).reversed()
                .thenComparing(Map.Entry::getKey));

        int limit = Math.min(topN, entries.size());
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, Long> e = entries.get(i);
            TopPageResult result =
                    new TopPageResult(windowStart, windowEnd, i + 1, e.getKey(), e.getValue());
            LOG.info("topn-pages emit: {}", result);
            out.collect(result);
        }

        // 가드레일 #6: 계산이 끝난 큰 state는 즉시 clear. 작은 emitted 플래그만 남겨(late 재발화 무시),
        // 그 플래그는 State TTL이 나중에 청소한다.
        pageCounts.clear();
        windowStartState.clear();
        emittedState.update(true);
    }
}