# HANDOFF — Flink 실시간 분석 파이프라인 (연습 프로젝트)

> 작업 인수인계 문서. 현재까지의 구현 상태 · 검증 결과 · 다음 할 일 · 환경 제약을 정리한다.
> 최종 갱신: **2026-06-15** (`2차 전체 완료` — Top N · DLQ · late · State TTL · 견고성(backpressure/복구). **1차(S1~R3) + 2차 전 단계 E2E 검증 완료**) / 현재 HEAD: `9652396 feat: 2차 전체 …`

---

## 1. 한 줄 요약

Kafka(Avro) 사용자 행동 이벤트를 Flink로 실시간 집계(5분 윈도우 page click count + 윈도우별 Top N)해 OpenSearch에 적재하는 E2E 스트림 파이프라인. 역직렬화 실패(DLQ)·늦은 이벤트(late)·State TTL·장애 복구까지 갖춘 견고성 단계 포함.
**전체 스펙·가드레일은 [`CLAUDE.md`](../CLAUDE.md)가 단일 출처(SSOT)** — 이 문서는 진행 상황 스냅샷이다.

## 2. 진행 상황 (CLAUDE.md 체크리스트 기준)

### 1차 — Source → Process → Sink → Web UI → Runtime

| 단계 | 내용 | 상태 |
|---|---|---|
| **S1** | `user-activity-event.avsc` 정의 → `UserActivityEvent` codegen | ✅ 완료 |
| **S2** | docker-compose 기동(Kafka+SR+OpenSearch+Dashboards) + 토픽 생성 | ✅ 완료 |
| **S3** | Avro producer로 샘플 이벤트 적재 | ✅ 완료 |
| **S4** | KafkaSource + Confluent Avro deser → `print()` | ✅ 완료 |
| **S5** | Event Time / Watermark 부여(30s out-of-orderness) | ✅ 완료 |
| **P1·P2·P3** | filter-click → keyBy(pageId) → 5min tumbling window → `PageClickCount` | ✅ 완료 |
| **K1·K2·K3** | `PageClickCount` → JSON 문서(deterministic id) → OpenSearch sink(bulk/retry) + 멱등성 | ✅ 완료 |
| **W1** | 로컬 MiniCluster Flink Web UI(port 8082) | ✅ 완료 |
| **R1** | Checkpoint(60s, EXACTLY_ONCE, retain, file://) + Kafka offset 연동 | ✅ 완료 |
| **R2** | 전 operator name/uid 점검 | ✅ 완료 |
| **R3** | E2E 검증(produce → agg 인덱스) | ✅ 완료 |

### 2차 — Top N → 견고성(DLQ/late) → 운영(TTL/backpressure)

| 단계 | 내용 | 상태 |
|---|---|---|
| **Top N (Process)** | `keyBy(windowEnd)` → `TopNPagesFunction` → `TopPageResult` `[topn-pages]` | ✅ 완료 |
| **Top N (Sink)** | OpenSearch `user-activity-topn` 저장 `[sink-opensearch-topn]` | ✅ 완료 |
| **DLQ** | Avro 역직렬화 실패 → side output → `user-activity-dlq` | ✅ 완료 |
| **late** | `allowedLateness(60s)` + lateData side output → `user-activity-late` (+ `withIdleness`) | ✅ 완료 |
| **State TTL** | TopN MapState/ValueState에 `StateTtlConfig`(1h) | ✅ 완료 |
| **견고성** | backpressure 주입(`SINK_DELAY_MS`) + 장애 복구(fixed-delay restart + fault 주입) | ✅ 완료 |

> **1차 + 2차 전 단계 완료.** 남은 것은 "완료 기준" 중 일부 데모(스키마 진화 호환성·실 클러스터 제출) — §5 참조.

> **작업 규칙(중요):** 사용자는 **한 스텝씩만** 구현하고 멈추길 원함. 검증 게이트 통과 후 다음 진행. 임의로 다음 스텝 선행 금지.

## 3. 아키텍처 — 현재 DAG (2차 완성형)

```text
Kafka(user-activity-events, Avro + plain-junk 혼재)
  → source-user-activity-events        (raw byte[] 읽기, RawBytesDeserializationSchema)
  → split-deser                        (Avro deser try/catch; 실패 → DLQ side output)
      ├─(DLQ)──────────────────────────→ sink-opensearch-dlq      [user-activity-dlq]
      └─(main)
        → assign-watermark             (Event Time, 30s OoO, withIdleness 10s)
        → filter-click                 (eventType == CLICK)
        → [fault-injection]            (선택: FAIL_AFTER>0일 때만; 복구 데모)
        → keyBy(pageId)
        → window-pageclick-5m          (5min tumbling, allowedLateness 60s, late side output)
            ├─(late)──────────────────→ sink-opensearch-late      [user-activity-late]
            ├─(main)──────────────────→ sink-opensearch-agg       [user-activity-agg]
            └─(main) → keyBy(windowEnd)
                       → topn-pages     (KeyedProcessFunction, 상위 N bounded, State TTL, clear)
                       → sink-opensearch-topn                      [user-activity-topn]
```

> **2차 구조 변경 핵심:** 1차는 source에 `forSpecific` Avro deser를 직접 물렸지만(S4), `DeserializationSchema`는 side output을 못 내므로 DLQ를 위해 **source는 raw byte[]만 읽고**(`RawBytesDeserializationSchema`) **Avro 역직렬화를 `split-deser`(ProcessFunction)로 한 단계 미뤘다**. `forSpecific` 자체는 그대로 사용, 위치만 이동.

OpenSearch 인덱스/문서 ID:

| 인덱스 | 원본 | 문서 ID(deterministic) |
|---|---|---|
| `user-activity-agg` | `PageClickCount` | `pageId_windowStart_windowEnd_CLICK` |
| `user-activity-topn` | `TopPageResult` | `windowStart_windowEnd_rank_pageId` |
| `user-activity-dlq` | `DlqRecord` | 원본 value 바이트 **SHA-256** |
| `user-activity-late` | `UserActivityEvent` | `eventId` |

## 4. 지금까지 만든 것 (파일)

```text
filnk-practice/
├── CLAUDE.md                          # 프로젝트 스펙·가드레일 (SSOT)
├── docker-compose.yml                 # S2: Kafka(KRaft)+SR+OpenSearch 2.11.1+Dashboards
├── pom.xml                            # codegen + producer(exec:java / @poison) + Flink/OpenSearch 의존성, jackson-bom 고정
├── docs/
│   ├── HANDOFF.md                     # (이 문서)
│   └── pipeline-class-guide.md        # 별개 학습용 가이드(클래스별 설명)
└── src/main/
    ├── avro/
    │   └── user-activity-event.avsc           # codegen source → UserActivityEvent
    ├── resources/
    │   └── log4j2.properties                  # 파이프라인 관측 로깅(slf4j→log4j2)
    └── java/com/example/flink/
        ├── producer/
        │   ├── SampleEventProducer.java        # S3: 정상 Avro 샘플 이벤트 적재(exec:java)
        │   └── PoisonEventProducer.java        # 2차 DLQ: 비-Avro 평문 메시지 적재(exec:java@poison)
        ├── source/
        │   ├── UserActivityKafkaSourceFactory.java # KafkaSource<byte[]> (raw bytes, offset checkpoint 연동)
        │   ├── RawBytesDeserializationSchema.java  # 2차: value를 byte[] 그대로 통과(side output 위해)
        │   └── AvroDeserSplitter.java          # 2차 DLQ: forSpecific try/catch → 실패 시 DLQ side output
        ├── model/
        │   ├── PageClickCount.java             # P3: 윈도우 집계 결과 POJO
        │   ├── TopPageResult.java              # 2차: Top N 결과 POJO(rank 1..N)
        │   └── DlqRecord.java                  # 2차: 역직렬화 실패 메시지(원본 bytes + 에러) POJO
        ├── agg/
        │   ├── PageClickCountAggregator.java   # P3: 증분 count AggregateFunction
        │   └── PageClickWindowResultFunction.java # P3: 윈도우 메타 부착 ProcessWindowFunction
        ├── topn/
        │   └── TopNPagesFunction.java          # 2차: keyBy(windowEnd) → 상위 N(MapState+타이머, clear, TTL)
        ├── ops/
        │   └── FaultInjectionMapper.java       # 2차 견고성: FAIL_AFTER 시 1회 의도적 throw(복구 데모)
        ├── sink/
        │   ├── OpenSearchDocs.java             # 결과 POJO → JSON 문서 + deterministic doc id (agg/topn/dlq/late)
        │   └── OpenSearchSinkFactory.java      # Opensearch2Sink 팩토리 4종(agg/topn/dlq/late, bulk/retry)
        └── job/
            └── FlinkUserActivityAnalyticsJob.java  # DAG 조립 + env(Web UI/checkpoint/restart strategy)
target/generated-sources/avro/.../UserActivityEvent.java   # 빌드 산출(gitignore, 커밋 안 함)
```

---

### 1차 요약 (S1~R3) — 상세는 git 이력·코드 주석 참조

- **S1 Avro 스키마/codegen**: `UserActivityEvent`(eventId/userId/pageId/eventType/eventTime(long)/sessionId(nullable)), namespace `com.example.flink.model.avro`, `stringType=String`.
- **S2 인프라**: docker-compose 4서비스(kafka KRaft 7.6.1 / schema-registry 7.6.1 / opensearch 2.11.1 / dashboards). 토픽 `user-activity-events`(partitions 3, RF 1), named volume로 보존.
- **S3 producer**: `SampleEventProducer` — `KafkaAvroSerializer`, 첫 직렬화 시 subject `user-activity-events-value` 자동 등록, key=pageId, CLICK:VIEW≈3:1, eventTime 흩뿌림.
- **S4 KafkaSource**: (1차) `ConfluentRegistryAvroDeserializationSchema.forSpecific` → SpecificRecord. **2차에서 raw byte[] + split-deser로 리팩터**(§3, 아래 DLQ).
- **S5 Watermark `[assign-watermark]`**: `forBoundedOutOfOrderness(30s)` + Event Time = `eventTime`. **2차에서 `withIdleness(10s)` 추가**(아래 late).
- **P1·P2·P3 `[filter-click]`→keyBy(pageId)→`[window-pageclick-5m]`**: `TumblingEventTimeWindows.of(5min)` + `AggregateFunction`(증분 count) + `ProcessWindowFunction`(window 메타 부착) → `PageClickCount`. 증분 집계로 state 폭증 방지(가드레일 #2·#8).
- **K1·K2·K3 `[sink-opensearch-agg]`**: `Opensearch2Sink`, deterministic doc id `pageId_windowStart_windowEnd_CLICK`, JSON 본문(Jackson), bulk(500/2MB/2s) + 지수 retry(5/1s), `AT_LEAST_ONCE` + 멱등 id.
- **W1 Web UI**: `createLocalEnvironmentWithWebUI(conf)` + `RestOptions.PORT=8082`(8081은 SR 점유), `flink-runtime-web`(provided). Job 실행 중에만 생존.
- **R1 Checkpoint**: `enableCheckpointing(60s, EXACTLY_ONCE)`, timeout 60s / min-pause 30s / max-concurrent 1 / tolerable-failure 3 / `RETAIN_ON_CANCELLATION` / `file://` 저장소. Kafka offset의 source of truth = checkpoint(`commit.offsets.on.checkpoint=true`, auto-commit 비의존).
- **R2 name/uid**: 전 operator 부여(keyBy 제외). **R3 E2E**: produce→Kafka→Flink→agg 인덱스. count 합 == 전체 CLICK 수.

> ⚠️ **R3 이월 이슈(2차에서 해결)**: 무한 스트리밍에서 parallelism(12) > 파티션(3)이라 **idle source subtask가 watermark를 MIN으로 묶어 윈도우가 firing 안 됨**. → 2차 late 단계에서 `withIdleness`로 해결.

---

### 2차 — Top N (Process + Sink)

**`[topn-pages]` `topn/TopNPagesFunction.java` (KeyedProcessFunction<Long, PageClickCount, TopPageResult>)**
- `window-pageclick-5m`의 `PageClickCount` 스트림을 **`keyBy(windowEnd)`** → 윈도우별 클릭수 상위 N개 페이지를 `TopPageResult`(rank 1..N)로 방출.
- **상태**: `MapState<String,Long>`(pageId→count) + `ValueState<Long>`(windowStart) + `ValueState<Boolean>`(emitted).
- **동작**: 같은 windowEnd로 들어온 페이지 카운트를 MapState에 모으고 **`windowEnd+1` 이벤트타임 타이머** 등록. 워터마크가 윈도우 끝을 지나 타이머 발화 시 `(count desc, pageId asc)` 정렬 → 상위 N emit → **큰 state(MapState/windowStart) clear**(가드레일 #6).
- **late 재발화 무시**: `allowedLateness`로 윈도우가 종료 후 다시 firing되면 페이지 1건이 재유입되지만, `emitted` 플래그가 true면 Top N 재계산을 건너뜀 → Top N은 "윈도우 종료 시점 확정" 시맨틱(늦은 갱신은 agg 인덱스에만 반영).
- **tie-break**: 같은 클릭수면 pageId 사전순 → 같은 입력이면 항상 같은 rank/pageId → deterministic doc id로 멱등(K3 철학을 Top N에).
- **`model/TopPageResult.java`**: POJO(windowStart, windowEnd, rank, pageId, clickCount), Flink POJO 규칙 준수.

**`[sink-opensearch-topn]`**: `OpenSearchSinkFactory.topnSink(...)` → `user-activity-topn`, doc id `windowStart_windowEnd_rank_pageId`. agg와 동일 bulk/retry 설정 공유.

### 2차 — 견고성: DLQ (역직렬화 실패 격리)

가드레일 #9: "역직렬화 실패/스키마 불일치 이벤트는 파이프라인을 죽이지 말 것."

- **`source/RawBytesDeserializationSchema.java`**: source는 value를 `byte[]` 그대로 통과(`DeserializationSchema`는 side output 불가 → Avro deser를 뒤로 미루는 이유).
- **`source/AvroDeserSplitter.java` `[split-deser]` (ProcessFunction<byte[], UserActivityEvent>)**:
  - `open()`에서 `ConfluentRegistryAvroDeserializationSchema.forSpecific(...)` 생성 + `inner.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(...))`로 초기화.
  - `processElement`: **try/catch는 `inner.deserialize(value)`만** 감싼다. 성공 → main output, 실패 → `DLQ_TAG` side output(`DlqRecord`(원본 bytes + 에러 문자열)). ⚠️ `out.collect()`까지 감싸지 않음 — 하류(window/sink) 예외를 "역직렬화 실패"로 오인하면 정상 이벤트를 DLQ로 보내고 장애도 삼켜 restart가 안 일어남.
- **`model/DlqRecord.java`**: POJO(rawValue byte[], length, error). 일부러 Avro 타입을 담지 않아 깨끗한 Flink POJO 직렬화(Kryo-on-Avro 회피).
- **`[sink-opensearch-dlq]`**: `user-activity-dlq`, doc id = 원본 바이트 **SHA-256**(같은 독성 메시지 재소비 시 덮어쓰기 멱등), 본문에 error/length/rawBase64.
- **`producer/PoisonEventProducer.java`**(`exec:java@poison`): 일부러 `StringSerializer`로 평문 메시지 적재 → Confluent Avro magic byte(0) 불일치로 deser 실패 유발. 정상 Avro와 한 토픽에 섞여도 정상 이벤트는 집계되고 독성만 DLQ로 분리됨을 보여 줌.

### 2차 — 견고성: late event (늦은 이벤트 처리)

- **`window-pageclick-5m`**: `.allowedLateness(60s)` + `.sideOutputLateData(LATE_TAG)`.
  - allowedLateness 내 늦은 이벤트 → 윈도우 **재발화**(agg 인덱스가 갱신; Top N은 emitted 플래그로 무시).
  - allowedLateness **초과**로 버려질 이벤트 → `LATE_TAG` side output → `[sink-opensearch-late]` `user-activity-late`(doc id = eventId).
- **`assign-watermark`에 `withIdleness(10s)` 추가**: R3 이월 이슈 해결. 데이터 없는 source subtask를 idle로 표시해 워터마크 정체를 풀어 **무한 스트리밍에서도 윈도우가 firing**되게 함.

### 2차 — 운영: State TTL

- `TopNPagesFunction.open()`에서 MapState/ValueState 3개 모두에 `StateTtlConfig`(기본 **1h**, `OnCreateAndWrite` / `NeverReturnExpired`) 적용. `STATE_TTL_HOURS` env override.
- 정상 흐름은 타이머 발화 시 큰 state를 clear하지만, **타이머가 끝내 발화하지 못한 windowEnd 키**(워터마크 미진행 등)의 state 누수를 막는 **안전망**. 윈도우+lateness보다 충분히 길게 둠.

### 2차 — 운영: backpressure + 장애 복구

- **backpressure 주입**: `OpenSearchSinkFactory.aggSink(..., sinkDelayMs)` — `SINK_DELAY_MS>0`이면 agg sink emit마다 `Thread.sleep`으로 지연 → bulk 큐가 차 상류로 backpressure 전파(Web UI 관찰). ⚠️ 단, windowing이 sink 앞 볼륨을 크게 줄이고 sink가 window에 chain돼 **BP 지표는 약하게** 나타남(검증에서 throttle 자체는 확인).
- **장애 복구**: `conf`에 `RestartStrategyOptions` fixed-delay(**3회 / 5s**) 설정. **`ops/FaultInjectionMapper.java`**(`FAIL_AFTER>0`일 때만 DAG에 삽입) — JVM 전역 카운터(`AtomicLong`)가 `FAIL_AFTER`에 도달하면 `AtomicBoolean`으로 **1회만** 의도적 throw. restart strategy가 job을 재시작 → **마지막 checkpoint에서 Kafka offset + window/Top N state 함께 복원** → 멱등 sink로 정상 결과 복구.
  - ⚠️ static 플래그는 JVM 전역(로컬 MiniCluster는 JM/TM 한 JVM)이라 재시작 후엔 true → 무한 재실패 방지. 이는 **장애 주입 제어용**이지 비즈니스 상태가 아니므로 가드레일 #2(상태는 Flink State로)와 무관.

### Job env 설정 / 환경변수

`FlinkUserActivityAnalyticsJob`의 env override:
`BOOTSTRAP_SERVERS` / `SCHEMA_REGISTRY_URL` / `KAFKA_GROUP_ID` / `BOUNDED` / `OPENSEARCH_HOST|PORT|SCHEME` / `WEB_UI_PORT`(8082) / `CHECKPOINT_DIR`
**2차 추가**: `TOP_N`(3) / `ALLOWED_LATENESS_MS`(60000) / `IDLENESS_MS`(10000) / `STATE_TTL_HOURS`(1) / `SINK_DELAY_MS`(0) / `FAIL_AFTER`(0).

## 5. 재현 — 어떻게 띄우고 검증하나

> ⚠️ 이 머신은 **colima** 런타임. `docker compose`(공백)가 아니라 **`docker-compose`(하이픈)** 사용. (자세한 환경 제약은 §7)

```bash
# (A) 인프라 기동
docker-compose up -d

# (B) 토픽 생성 (최초 1회 — 이미 생성됨, named volume로 보존)
docker-compose exec kafka kafka-topics --create \
  --topic user-activity-events --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# (C) 정상 Avro 샘플 이벤트 적재 (compile 시 codegen 포함)
mvn -q compile exec:java                  # 기본 60건
mvn -q compile exec:java -Dexec.args=200  # 건수 지정

# (D) Flink Job 실행 — forked JVM. 실행 중 Web UI = http://localhost:8082
BOUNDED=true mvn -q compile exec:exec@job # 시작 시점까지 읽고 종료(E2E 검증용)
mvn -q compile exec:exec@job              # 무한 스트리밍(실제 파이프라인 + Web UI). Ctrl+C 종료
```

### 1차 검증 커맨드 (요약)
```bash
# P3/K2: 윈도우 집계 + agg 인덱스 (count 합 == 전체 CLICK)
BOUNDED=true mvn -q compile exec:exec@job
curl -s -XPOST localhost:9200/user-activity-agg/_refresh
curl -s localhost:9200/user-activity-agg/_count
curl -s localhost:9200/user-activity-agg/_search -H 'Content-Type: application/json' \
  -d '{"size":0,"aggs":{"total_clicks":{"sum":{"field":"count"}}}}'

# W1: Web UI (무한 스트리밍 중)
mvn -q compile exec:exec@job &
curl -s -o /dev/null -w '%{http_code}\n' localhost:8082/         # 200
curl -s localhost:8082/jobs/overview                              # state RUNNING

# R1: checkpoint + offset (무한 스트리밍 ~60s 후)
jid=$(curl -s localhost:8082/jobs/overview | grep -oE '"jid":"[a-f0-9]+"' | head -1 | grep -oE '[a-f0-9]{20,}')
curl -s "localhost:8082/jobs/$jid/checkpoints" | python3 -c "import sys,json;print(json.load(sys.stdin)['counts'])"
find /tmp/flink-checkpoints/user-activity-analytics -name 'chk-*' -type d
docker-compose exec -T kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group flink-user-activity-analytics                # LAG 0
```

### 2차 Top N 검증
```bash
BOUNDED=true mvn -q compile exec:exec@job
curl -s -XPOST localhost:9200/user-activity-topn/_refresh
curl -s localhost:9200/user-activity-topn/_count                  # 윈도우 × N
# rank가 agg 상위 N과 일치 + 동점 시 pageId 사전순 tie-break
curl -s 'localhost:9200/user-activity-topn/_search?size=20&sort=clickCount:desc'
```

### 2차 DLQ 검증
```bash
# 독성(비-Avro) 메시지 5건 적재
mvn -q compile exec:java@poison                       # 기본 5건 (-Dexec.args=10 으로 조정)
BOUNDED=true mvn -q compile exec:exec@job             # 정상 이벤트는 집계, 독성은 DLQ로 — 무중단
curl -s -XPOST localhost:9200/user-activity-dlq/_refresh
curl -s localhost:9200/user-activity-dlq/_count       # = poison 건수
curl -s 'localhost:9200/user-activity-dlq/_search?size=1'   # error="Magic number does not match", rawBase64
```

### 2차 late 검증
```bash
# eventTime을 과거로 backdate한 이벤트를 흘려보낸 뒤 (또는 무한 스트리밍에서 watermark가 충분히 진행한 뒤)
curl -s -XPOST localhost:9200/user-activity-late/_refresh
curl -s localhost:9200/user-activity-late/_count      # allowedLateness 초과분
```

### 2차 backpressure / 장애 복구 검증
```bash
# backpressure: agg sink emit 지연 주입 → Web UI 8082에서 throttle 관찰
SINK_DELAY_MS=500 mvn -q compile exec:exec@job

# 복구: FAIL_AFTER 레코드 처리 후 1회 의도적 장애 → fixed-delay restart → checkpoint 복구
FAIL_AFTER=300 mvn -q compile exec:exec@job
# 로그: "fault-injection: 전역 N번째 레코드에서 의도적 장애 주입" → ExecutionGraph restart → 정상 결과 복구
```

### 검증 결과 (통과 ✔)

| 항목 | 기대값 / 실측 |
|---|---|
| S1 codegen | `UserActivityEvent.java` 생성, `getEventTime()`→long, 나머지 String |
| S2 토픽 | `user-activity-events` Partitions 3 |
| S3 Schema Registry | subject `user-activity-events-value`(v1) 등록, offset 합 = 적재 건수 |
| S4 read | SpecificRecord 역직렬화, 적재 건수만큼 출력 |
| S5 watermark | `recordTs == eventTime` 전건 일치(timestamp assigner 동작) |
| P1·P2·P3 window | **윈도우 count 합 == 전체 CLICK 수**(실측 255==255, VIEW 65 전량 제외) |
| K1·K2 sink | agg 문서 24건, count 합 255, `_id`=deterministic |
| K3 멱등성 | 2회 실행 → 문서 수 24 불변, `_version` 1→2(덮어쓰기), count 동일 |
| W1 Web UI | `/` 200, `/jobs/overview` RUNNING, vertices에 operator name/uid 라벨 노출; 종료 후 8082 000 |
| R1 checkpoint | `completed≥1`, `chk-N` 디렉터리 존재, 로그 `Completed checkpoint N`, kill 후 chk dir 보존 |
| R1 offset 연동 | consumer group `CURRENT==LOG-END`(LAG 0), checkpoint와 함께 commit |
| R2 name/uid | 전 operator 부여(keyBy 제외), Web UI 라벨 이중 확인 |
| R3 E2E | agg 문서 34건, **count 합 401 == 전체 CLICK 401** |
| **2차 Top N** | `user-activity-topn` **3문서**, rank가 agg 상위 3과 일치, 동점 시 pageId 사전순 tie-break, 재실행 시 `_version`만 증가(멱등) |
| **2차 DLQ** | poison 5건 → `"Magic number does not match"` → `user-activity-dlq` **5문서**, **파이프라인 무중단**(정상 이벤트는 정상 집계) |
| **2차 late** | backdate 이벤트 → `user-activity-late` **22문서**, `withIdleness`로 스트리밍에서도 윈도우 firing |
| **2차 State TTL** | TopN state에 `StateTtlConfig` 적용(타이머 미발화 키 누수 안전망) |
| **2차 backpressure** | `SINK_DELAY_MS=500` → 500ms 간격 throttle 확인(단 windowing+chain으로 BP 지표 자체는 약함) |
| **2차 복구** | fault 발생 → ExecutionGraph 재배포(restart) → **멱등 sink로 정상 결과 복구**(agg 합/topn/dlq 정상) |
| OpenSearch | `_cluster/health` green, nodes 1 |
| Dashboards | `localhost:5601/api/status` 200 |

## 6. 다음 할 일

> **1차 + 2차 구현은 전부 완료.** 아래는 CLAUDE.md "완료 기준" 중 아직 **데모로 보여 주지 않은** 항목과 선택적 확장이다.

- [ ] **스키마 진화 호환성 데모**(완료 기준): `.avsc`에 nullable 필드를 추가하고 BACKWARD 호환으로 register → 기존 reader(`forSpecific`)가 깨지지 않고 read되는지 확인. (현재 미실시)
- [ ] **실 클러스터 제출**: 현재 `createLocalEnvironmentWithWebUI`는 로컬 MiniCluster 전용. `flink run` 제출하려면 (1) fat-jar(maven-shade-plugin), (2) env 생성을 로컬/클러스터로 분기. 후순위.
- [ ] (선택) **RocksDB state backend** 교체 여지(대용량 state 대비) — 현재 기본 HashMap.
- [ ] (선택) backpressure를 더 뚜렷하게 보려면 sink를 window에서 **chain 분리**(`disableChaining`/`startNewChain`)하거나 sink parallelism을 높여 관찰.

> 참고: AT_LEAST_ONCE sink는 checkpoint 시점에 pending bulk를 flush → sink flush가 60초 checkpoint와 정렬됨(멱등 id 덕에 정확성은 이미 보장).

## 7. 환경 제약 (Apple Silicon + colima) — 반드시 숙지

> 메모리 [[local-infra-env]] 와 동일 내용. 이 제약들을 모르면 인프라 기동에서 막힌다.

1. **`docker compose`(v2 플러그인) 없음 → `docker-compose`(하이픈, standalone v2.32.0)만 동작.** CLAUDE.md 빌드 가이드는 `docker compose`(공백)로 적혀 있으니 치환 필요.
2. **OpenSearch는 2.11.1로 핀.** 2.12+ 이미지의 번들 JDK21이 Apple Silicon Docker에서 `SIGILL` 크래시(exit 134). 2.11.1은 JDK17 번들이라 정상.
3. **colima 메모리 ≥ 약 6~8GB 필요.** 기본 2GB에선 OpenSearch OOM kill(exit 137). 현재 **7.7GB**로 상향해 green 확인. colima 메모리 변경 시 재시작 → 전 컨테이너 SIGTERM(143) → `docker-compose up -d` 재기동 필요.
4. Confluent 이미지라 토픽 CLI는 **`kafka-topics`**(`.sh` 없음). CLAUDE.md는 `kafka-topics.sh`로 적혀 있음.
5. kafka/opensearch는 named volume(`kafka-data`, `opensearch-data`) → 재시작해도 토픽/데이터 보존.

### 실행 함정 (해결됨)

- **Flink는 `provided`** scope라 (1) `exec`는 `classpathScope=compile`로 provided까지 포함, (2) `flink-connector-kafka`의 provided 전이의존 누락분 `flink-connector-base` 명시 추가, (3) `exec:java`(동일 JVM)는 MiniCluster classloader 문제로 `ExecutionConfig ClassNotFound` → **Flink Job은 `exec:exec`(forked JVM)** 로 실행. producer=`exec:java`, poison producer=`exec:java@poison`, job=`exec:exec@job`.
- **OpenSearch 커넥터 좌표**: `flink-connector-opensearch2:2.0.0-1.18`(OpenSearch 2.x용; `1.2.0-1.18`은 1.x용으로 혼동 주의). 클라이언트 2.13.0 번들 → 서버 2.11.1과 2.x 호환.
- **jackson 충돌**: avro 1.11.3이 `jackson-core:2.14.2`를 전이로 끌어와 opensearch 클라이언트(2.17.0)와 충돌(`NoClassDefFoundError`) → `dependencyManagement`로 `jackson-bom:2.17.0` import해 전 모듈 고정.
- **`-Dexec.mainClass` override 함정**: exec 플러그인의 하드코딩 `<mainClass>`(SampleEventProducer)가 `-Dexec.mainClass`를 덮어 PoisonEventProducer가 안 떴음 → 별도 execution `@poison`으로 분리.

## 8. Git

- 원격: `git@github.com:pado0/flink-user-event-pipeline.git` (SSH), `origin/main` 추적.
- 최근 커밋 이력(요약):
  - … (S1~K3, W1, 로깅)
  - `02979cb` `feat: R1·R2·R3 Runtime (60s checkpoint + Kafka offset 연동 + name/uid 점검 + E2E)`
  - `9652396` `feat: 2차 전체 — Top N · DLQ · late · State TTL · 견고성(backpressure/복구)` ← **현재 HEAD**
- `9652396` 변경 파일: `job/FlinkUserActivityAnalyticsJob.java`(DAG 재조립), `topn/TopNPagesFunction.java`·`model/TopPageResult.java`·`model/DlqRecord.java`·`ops/FaultInjectionMapper.java`·`source/AvroDeserSplitter.java`·`source/RawBytesDeserializationSchema.java`·`producer/PoisonEventProducer.java`(신규), `sink/OpenSearchDocs.java`·`sink/OpenSearchSinkFactory.java`·`source/UserActivityKafkaSourceFactory.java`·`producer/SampleEventProducer.java`(수정), `pom.xml`(poison execution), `CLAUDE.md`(2차 체크리스트 ✅), `docs/pipeline-class-guide.md`(신규 학습 가이드).
- ⚠️ **이 HANDOFF.md 갱신은 아직 미커밋**(working tree). 위 2차 커밋에는 HANDOFF 갱신분이 빠져 있으니, 이번 문서 갱신을 별도 `docs:` 커밋으로 남기길 권장.

## 9. 참고

- 단일 출처 스펙: [`CLAUDE.md`](../CLAUDE.md) — DAG, 가드레일, OpenSearch 인덱스/문서 ID, 체크리스트 전문.
- 클래스별 학습 가이드: [`docs/pipeline-class-guide.md`](pipeline-class-guide.md).
- 관련 메모리: `project-progress`, `local-infra-env`, `flink-local-run`, `opensearch-sink-deps`, `logging-log4j2-setup` (`~/.claude/.../memory/`).