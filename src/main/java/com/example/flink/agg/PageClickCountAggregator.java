package com.example.flink.agg;

import com.example.flink.model.avro.UserActivityEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * P3 — 윈도우 내 클릭 이벤트 개수를 <b>증분(incremental)</b>으로 집계하는 {@link AggregateFunction}.
 *
 * <p>증분 집계라 윈도우에 원본 이벤트를 쌓아두지 않고 누적값(Long) 하나만 상태로 유지한다
 * → state/메모리 효율적(가드레일: 상태는 Flink State로). 윈도우 메타데이터(windowStart/End)
 * 부착은 {@link PageClickWindowResultFunction}이 담당한다 — {@code aggregate(aggFn, windowFn)} 조합.
 *
 * <p>제네릭: IN={@link UserActivityEvent}, ACC=Long(누적 카운트), OUT=Long(최종 카운트).
 */
public final class PageClickCountAggregator
        implements AggregateFunction<UserActivityEvent, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserActivityEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        // 윈도우 병합(session window 등) 시 사용. tumbling에선 호출되지 않지만 계약상 구현해 둔다.
        return a + b;
    }
}
