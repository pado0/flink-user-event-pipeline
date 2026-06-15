package com.example.flink.ops;

import com.example.flink.model.avro.UserActivityEvent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2차 견고성(데모 전용) — checkpoint 복구를 눈으로 확인하기 위해 스트림 중간에서 <b>일부러 한 번</b>
 * 예외를 던져 task를 죽이는 pass-through map. {@code FAIL_AFTER} 레코드를 처리한 뒤 한 번만 throw하고,
 * restart strategy로 job이 재시작되면 마지막 checkpoint에서 Kafka offset + window/Top N state가
 * 함께 복원된다(가드레일: 상태/오프셋 일관 복원).
 *
 * <p>DAG 라벨 {@code [fault-injection]}. {@code FAIL_AFTER<=0}이면 DAG에 끼우지 않는다(평시 무영향).
 *
 * <p>한 번만 죽이기 위한 {@link AtomicBoolean}은 JVM 전역(static)이다 — 로컬 MiniCluster는 JM/TM이
 * 한 JVM이라 재시작 후에도 플래그가 살아 있어 무한 재실패를 막는다. 이는 <b>장애 주입 제어용</b>이지
 * 파이프라인 비즈니스 상태가 아니므로 "상태는 Flink State로"(가드레일 #2)와 무관하다.
 */
public final class FaultInjectionMapper extends RichMapFunction<UserActivityEvent, UserActivityEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FaultInjectionMapper.class);

    /** JVM당 한 번만 발화. 재시작 후엔 true라 통과 → 복구 성공을 관찰할 수 있다. */
    private static final AtomicBoolean FIRED = new AtomicBoolean(false);

    /** JVM 전역 카운터: FAIL_AFTER는 (subtask별이 아니라) 전체 처리 건수 기준. MiniCluster=한 JVM. */
    private static final AtomicLong SEEN = new AtomicLong(0);

    private final long failAfter;

    public FaultInjectionMapper(long failAfter) {
        this.failAfter = failAfter;
    }

    @Override
    public UserActivityEvent map(UserActivityEvent value) {
        long n = SEEN.incrementAndGet();
        if (n >= failAfter && FIRED.compareAndSet(false, true)) {
            LOG.warn("fault-injection: 전역 {}번째 레코드에서 의도적 장애 주입(checkpoint 복구 데모)", n);
            throw new RuntimeException(
                    "injected fault after " + n + " records (checkpoint-restore demo)");
        }
        return value;
    }
}
