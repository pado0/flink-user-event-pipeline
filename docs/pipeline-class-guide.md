# Flink 사용자 행동 분석 파이프라인 — 클래스별 동작 가이드

> 이 문서는 현재까지 **구축된** 파이프라인을 클래스 단위로 "교과서처럼" 설명한다.
> 1차(`S1`~`R3` + `W1` Web UI)에 더해 **2차(Top N · DLQ · late · State TTL · 견고성)** 까지 모두 코드가 있다.
> 1차는 1~9장, 2차는 11장에 정리한다. 코드 기준: `src/main/java/com/example/flink/**`, `src/main/avro/user-activity-event.avsc`.

---

## 0. 한눈에 보기 — 전체 데이터 흐름

```text
[외부 producer — Flink DAG 밖]
  SampleEventProducer  : 정상 Avro 이벤트 → Kafka(user-activity-events)   [첫 직렬화 때 Schema Registry에 스키마 등록]
  PoisonEventProducer  : 비-Avro 평문(DLQ 테스트용) → 같은 토픽

[Flink Job: FlinkUserActivityAnalyticsJob 가 아래 DAG를 조립·실행]

  (1) source-user-activity-events  — KafkaSource: 메시지를 원본 byte[]로만 읽음(해석 안 함)
  (2) split-deser                  — Avro 역직렬화 try/catch → UserActivityEvent
        +- (실패) DLQ side output  → DlqRecord(원본 bytes+에러) → sink-opensearch-dlq → 인덱스 user-activity-dlq
  (3) assign-watermark             — Event Time = eventTime, 30s out-of-orderness + withIdleness
  (4) filter-click                 — eventType == "CLICK" 만 통과
  (5) keyBy(pageId)                — 동일 pageId → 동일 task (KeyedStream)
  (6) window-pageclick-5m          — 5분 tumbling + allowedLateness(60s), 증분 count → PageClickCount(POJO)
        +- (side output) 늦은 이벤트 → sink-opensearch-late → 인덱스 user-activity-late
        +- (main) PageClickCount 스트림은 두 갈래로 분기:
              +- (7) sink-opensearch-agg  → 인덱스 user-activity-agg
              +- (8) keyBy(windowEnd) → topn-pages (상위 N, KeyedProcessFunction) → TopPageResult
                       +- (9) sink-opensearch-topn → 인덱스 user-activity-topn
```

> 1차 대비 2차 변경 요점: (1) source가 Avro를 직접 풀던 것을 **raw byte[]** 로 바꾸고 (2) `split-deser`에서
> 역직렬화(+DLQ side output)하도록 한 단계 미뤘다(DeserializationSchema는 side output 불가).
> 그리고 window에 allowedLateness/late side output, 뒤에 Top N + 세 개의 새 sink(dlq/late/topn)가 붙었다.

데이터의 **타입이 변하는 지점**이 곧 클래스의 경계다. 입력은 **Avro**(`UserActivityEvent`),
중간 집계 결과는 **POJO**(`PageClickCount`), 최종 적재는 **JSON 문서**다
(가드레일 #3: *입력 Avro / 출력 JSON*).

| 단계 | 입력 타입 | 출력 타입 | 담당 클래스 |
|---|---|---|---|
| Source | Kafka 레코드 | `byte[]` (원본, 해석 안 함) | `UserActivityKafkaSourceFactory` + `RawBytesDeserializationSchema` |
| Deser/DLQ | `byte[]` | `UserActivityEvent` (Avro) / 실패 시 `DlqRecord` | `AvroDeserSplitter` |
| Watermark/Filter | `UserActivityEvent` | `UserActivityEvent` | (Job 내 람다) |
| Window 집계 | `UserActivityEvent` | `Long` → `PageClickCount` | `PageClickCountAggregator` + `PageClickWindowResultFunction` |
| late side output | `UserActivityEvent` | `UserActivityEvent` | window `.sideOutputLateData(LATE_TAG)` |
| Top N | `PageClickCount` | `TopPageResult` | `TopNPagesFunction` |
| 문서 매핑 | 각 결과 POJO | `byte[]` JSON + doc id | `OpenSearchDocs` |
| Sink | 각 결과 POJO | OpenSearch `IndexRequest` | `OpenSearchSinkFactory` |

---

## 1. 스키마 — `user-activity-event.avsc` & 생성 클래스 `UserActivityEvent`

**역할**: 파이프라인 입력 이벤트의 단일 진실 원천(single source of truth). `.avsc`(Avro 스키마)를
`avro-maven-plugin`이 `generate-sources` 단계에서 **`UserActivityEvent` (SpecificRecord) 자바 클래스**로
코드 생성한다(`target/generated-sources/avro/...`). 손으로 작성하는 클래스가 아니다.

**필드**

| 필드 | 타입 | 의미 |
|---|---|---|
| `eventId` | `string` | 이벤트 고유 ID |
| `userId` | `string` | 사용자 ID |
| `pageId` | `string` | 페이지 ID — **`keyBy` 키** |
| `eventType` | `string` | `CLICK` / `VIEW` 등 |
| `eventTime` | `long` | **Event Time**, epoch milliseconds |
| `sessionId` | `["null","string"]` | nullable 세션 ID |

**교과서 포인트**
- `stringType=String`으로 codegen → 필드가 `CharSequence`가 아니라 `java.lang.String`이라 비교/직렬화가 편하다.
- 이 스키마는 producer(쓰기)와 source(읽기)가 **Schema Registry를 통해 공유**한다. 메시지 본문에는
  스키마 전체가 아니라 *schema id*만 박히고, 실제 스키마는 Registry에서 조회한다 → 스키마 진화의 기반.

---

## 2. `producer/SampleEventProducer` — (외부) 학습용 데이터 생성기

> Flink DAG의 일부가 **아니다**. "입력 Avro"를 만들어 Kafka에 흘려 넣는 외부 유틸리티.

**역할**: 무작위 `UserActivityEvent`를 Kafka `user-activity-events` 토픽에 적재한다.

**동작 핵심**
- value serializer = Confluent `KafkaAvroSerializer`. **첫 직렬화 때 스키마를 Schema Registry
  subject `user-activity-events-value`(TopicNameStrategy)에 자동 등록**한다 → `S3` 검증 게이트 충족.
- `ProducerRecord`의 **key = pageId**. 즉 동일 pageId는 항상 동일 Kafka 파티션으로 간다 →
  나중 Flink `keyBy(pageId)`와의 일관성을 학습하기 위한 의도적 설계.
- `eventTime`을 "현재 - (0~2분)" 범위로 흩뿌린다 → out-of-order 이벤트를 만들어 **watermark/window**를 관찰.
- `eventType`은 `CLICK`을 3:1 비중으로 많이 생성 → filter 이후에도 집계할 데이터가 충분.
- `acks=all`, 콜백으로 성공/실패 카운트, 실패가 있으면 `System.exit(1)`.

**실행**: `mvn -q compile exec:java` (기본 60건) / `mvn -q compile exec:java -Dexec.args=200`.

---

## 3. `source/UserActivityKafkaSourceFactory` — Source (`S4`)

**역할**: Kafka 토픽을 읽는 `KafkaSource`를 만든다. DAG 라벨 `[source-user-activity-events]`.

> **2차 변경(중요)**: 1차에서는 이 source에 `ConfluentRegistryAvroDeserializationSchema.forSpecific(...)`을
> 직접 물려 **Avro를 여기서 풀었다**. 2차에서는 DLQ를 위해 source가 `RawBytesDeserializationSchema`로
> **원본 byte[]만** 읽고, Avro 역직렬화는 side output이 가능한 `AvroDeserSplitter`(11-1)로 한 단계 옮겼다.
> (`DeserializationSchema`는 side output을 낼 수 없어 역직렬화 실패를 DLQ로 보낼 수 없기 때문 — 가드레일 #9.)

**핵심 메서드**: `create(bootstrapServers, schemaRegistryUrl, groupId, bounded)` → `KafkaSource<byte[]>`

```java
KafkaSource.<byte[]>builder()
    .setBootstrapServers(...)
    .setTopics("user-activity-events")
    .setGroupId(groupId)
    .setStartingOffsets(OffsetsInitializer.earliest())            // 처음부터 읽기
    .setProperty("commit.offsets.on.checkpoint", "true")          // R1: 오프셋 source of truth = checkpoint(이건 가시성용)
    .setValueOnlyDeserializer(new RawBytesDeserializationSchema()) // 2차: 해석 안 하고 byte[] 그대로
    // bounded=true 이면:
    .setBounded(OffsetsInitializer.latest())                       // 시작 시점 끝까지만 읽고 종료
```

**교과서 포인트**
- **왜 byte[]로?** Avro 역직렬화는 깨진 메시지(스키마 불일치 등)에 throw한다. source의
  `DeserializationSchema`에서 풀면 (가) 실패 시 파이프라인이 죽고 (나) side output(DLQ)을 낼 수 없다.
  그래서 deser를 `AvroDeserSplitter`(ProcessFunction)로 미뤄 try/catch + DLQ side output을 가능케 한다.
  `forSpecific` 자체는 그대로 쓰되 **위치만 한 단계 뒤로** 옮긴 것(가드레일 #9, #3은 11-1에서 충족).
- **`bounded` 스위치**가 중요하다.
  - `false`(기본): 무한 스트리밍. 실제 파이프라인 + Web UI 관찰용.
  - `true`: 시작 시점의 latest offset에 도달하면 source가 끝나 **job이 정상 종료** → E2E 검증/배치용.
- `setStartingOffsets(earliest())`: 최초 기동 시 토픽을 처음부터 읽는다(bounded 재실행마다 K3 멱등성/E2E
  반복 검증 가능). 단 checkpoint/savepoint에서 복구할 때는 이 설정과 무관하게 **checkpoint에 스냅샷된
  offset**에서 재개된다(`R1`).
- 가드레일: consumer group id 명시. offset은 Flink checkpoint와 함께 관리하고(`R1`)
  Kafka auto-commit에 의존하지 않는다.

> **파티션 ↔ 병렬성**: Kafka 파티션 수(3)가 source subtask 병렬성의 상한이다. source 병렬성이
> 파티션보다 많으면 남는 subtask는 idle.

---

## 4. `job/FlinkUserActivityAnalyticsJob` — DAG 조립 + 실행 환경 (`main`)

**역할**: 위/아래 모든 조각을 하나의 Flink DAG로 **조립**하고 실행 환경을 설정하는 진입점.
"파이프라인의 설계도"에 해당. 비즈니스 로직 자체는 거의 없고 *연결*이 일이다.

**실행 환경 (`W1`)**
```java
Configuration conf = new Configuration();
conf.set(RestOptions.PORT, 8082);                                  // 8081은 Schema Registry 점유
StreamExecutionEnvironment env =
    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
```
- 로컬 MiniCluster + **Web UI(http://localhost:8082)** 를 띄운다 → DAG·operator별
  backpressure·numRecordsIn/Out·watermark를 눈으로 관찰. **UI는 Job 실행 중에만 생존**하므로
  관찰하려면 `BOUNDED=false`로 띄워야 한다(bounded면 즉시 종료돼 접속 불가).

**DAG 조립 (operator마다 `.name()` + `.uid()` — 가드레일 #4)**

| # | 코드(요약) | operator name / uid | 의미 |
|---|---|---|---|
| (1) | `env.fromSource(source, noWatermarks(), ...)` | `source-user-activity-events` | Kafka에서 **원본 byte[]** 읽기(워터마크 미부여) |
| (2) | `.process(new AvroDeserSplitter(registry))` | `split-deser` | Avro 역직렬화(try/catch). 실패분 → `DLQ_TAG` side output → `sink-opensearch-dlq` |
| (3) | `.assignTimestampsAndWatermarks(forBoundedOutOfOrderness(30s)...withIdleness(...))` | `assign-watermark` | Event Time = `eventTime`, 30초 지연 + idleness |
| (4) | `.filter(e -> "CLICK".equals(e.getEventType()))` | `filter-click` | CLICK만 통과 |
| (선택) | `.map(new FaultInjectionMapper(failAfter))` | `fault-injection` | `FAIL_AFTER>0`일 때만 — 복구 데모용 1회 throw |
| (5) | `.keyBy(UserActivityEvent::getPageId)` | (키 분배) | 동일 pageId → 동일 task |
| (6) | `.window(Tumbling 5min).allowedLateness(60s).sideOutputLateData(LATE_TAG).aggregate(agg, windowFn)` | `window-pageclick-5m` | 5분 텀블링 집계 → `PageClickCount`. 늦은 이벤트 → `sink-opensearch-late` |
| (7) | `.sinkTo(OpenSearchSinkFactory.aggSink(...))` | `sink-opensearch-agg` | OpenSearch `user-activity-agg` 적재 |
| (8) | `.keyBy(PageClickCount::getWindowEnd).process(new TopNPagesFunction(...))` | `topn-pages` | 윈도우별 상위 N → `TopPageResult` |
| (9) | `.sinkTo(OpenSearchSinkFactory.topnSink(...))` | `sink-opensearch-topn` | OpenSearch `user-activity-topn` 적재 |

마지막에 `env.execute("flink-user-activity-analytics")`로 잡 트리거.

**교과서 포인트**
- **워터마크를 source가 아니라 (3) `assign-watermark`에서 부여**: 학습 의도로 단계를 분리. `noWatermarks()`로 source를
  통과시킨 뒤 `assign-watermark` operator에서 명시적으로 부여 → DAG에서 watermark 흐름이 또렷이 보임.
- 모든 operator에 `.uid()` 고정: 향후 checkpoint/savepoint에서 **상태 복원 키**가 되므로 필수.
- 환경변수로 모든 엔드포인트 override 가능: `BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`,
  `KAFKA_GROUP_ID`, `BOUNDED`, `OPENSEARCH_HOST/PORT/SCHEME`, `WEB_UI_PORT`.
  2차 추가: `TOP_N`, `ALLOWED_LATENESS_MS`, `IDLENESS_MS`, `STATE_TTL_HOURS`, `SINK_DELAY_MS`, `FAIL_AFTER`, `CHECKPOINT_DIR`.

---

## 5. 윈도우 집계 한 쌍 — `agg/PageClickCountAggregator` + `agg/PageClickWindowResultFunction`

(6) `window-pageclick-5m`은 `aggregate(aggregateFunction, processWindowFunction)` **조합**으로 만든다.
둘은 역할이 다르고, 함께 쓰면 "증분 집계의 효율"과 "윈도우 메타데이터 접근"을 모두 얻는다 — 전형적 패턴.

### 5-1. `PageClickCountAggregator` — 증분 카운터 (`AggregateFunction<UserActivityEvent, Long, Long>`)

| 메서드 | 동작 |
|---|---|
| `createAccumulator()` | `0L` — 누적기 초기화 |
| `add(event, acc)` | `acc + 1` — 이벤트 1건당 +1 |
| `getResult(acc)` | `acc` — 최종 카운트 |
| `merge(a, b)` | `a + b` — 윈도우 병합(session 등)용. tumbling에선 미호출이나 계약상 구현 |

**교과서 포인트**: 이것이 **증분(incremental) 집계**의 핵심이다. 윈도우에 원본 이벤트를 쌓지 않고
**누적값 `Long` 하나만 상태로 유지**한다 → state/메모리 효율(가드레일 #2: 상태는 Flink State로).
이벤트가 도착할 때마다 즉시 누적되고, 윈도우가 firing될 때 결과 1건만 흘러나온다.

### 5-2. `PageClickWindowResultFunction` — 윈도우 메타 부착 (`ProcessWindowFunction<Long, PageClickCount, String, TimeWindow>`)

```java
public void process(String pageId, Context ctx, Iterable<Long> counts, Collector<PageClickCount> out) {
    long count = counts.iterator().next();      // 증분 집계라 원소는 항상 1개
    TimeWindow w = ctx.window();
    out.collect(new PageClickCount(pageId, w.getStart(), w.getEnd(), count));
}
```

**교과서 포인트**
- 입력 `Iterable<Long>`의 원소는 **항상 1개**다 — 앞단 `AggregateFunction`이 이미 하나로 줄였기 때문.
  (만약 `ProcessWindowFunction` 단독으로 썼다면 윈도우의 *모든 이벤트*가 `Iterable`로 들어와 메모리를 더 씀.)
- `Context`에서 `window().getStart()/getEnd()`(윈도우 경계)와 워터마크·state에 접근한다.
- 여기서 비로소 `pageId + 윈도우 경계 + count`가 결합돼 결과 POJO `PageClickCount`가 완성된다.

---

## 6. `model/PageClickCount` — 집계 결과 모델 (POJO)

**역할**: (6) `window-pageclick-5m`의 산출물이자 (7) `sink-opensearch-agg` 적재의 원본. `pageId, windowStart, windowEnd, count` 4개 필드.

**교과서 포인트 — 왜 "POJO 규칙"을 지키나**
- `public` 무인자 생성자 + private 필드 + 표준 getter/setter. Flink `TypeExtractor`가 이를
  **POJO 타입**으로 인식하면, Kryo 같은 범용 직렬화 대신 **효율적인 POJO 직렬화**를 쓴다(성능·상태 크기 이득).
- `windowStart`(inclusive) / `windowEnd`(exclusive) 모두 epoch ms.
- `equals/hashCode/toString` 구현(검증·로깅 편의). `toString`은 epoch을 `Instant`로 사람이 읽게 출력.

---

## 7. `sink/OpenSearchDocs` — JSON 문서 매핑 + deterministic doc id (`K1`)

**역할**: `PageClickCount`(POJO) → OpenSearch 문서(JSON 바이트) + **문서 ID** 변환. 순수 매핑 유틸.

**두 핵심 메서드**

```java
// 문서 ID = pageId_windowStart_windowEnd_CLICK  ← deterministic
static String aggDocId(PageClickCount c)

// 문서 본문(JSON byte[]) : pageId, eventType, windowStart/End(+ISO 보조), count
static byte[] aggDocJson(PageClickCount c)
```

**교과서 포인트 — 멱등성의 출발점 (가드레일 #5)**
- OpenSearch는 트랜잭션 sink가 아니다. 그래서 exactly-once에 기대지 않고,
  **같은 윈도우 결과는 항상 같은 문서 ID**로 쓴다. 같은 ID로 다시 쓰면 OpenSearch가 *덮어쓰기(upsert)* →
  장애 재처리/재실행으로 같은 결과가 두 번 와도 **문서가 중복되지 않는다**(`K3` 멱등성).
- `eventType`은 (4) `filter-click`를 거쳐 항상 `CLICK`이므로 ID/필드에 상수로 박는다.
  (전제: pageId에 `_`가 없음 — 현재 데이터의 5종 페이지명은 안전.)
- 사람이 읽기 좋은 `windowStartIso/windowEndIso`(ISO-8601 UTC) 보조 필드를 함께 넣어 Dashboards에서 보기 편하게.
- `ObjectMapper`는 thread-safe라 `static`으로 재사용. 직렬화 예외는 sink `emit`이 checked를 못 던지므로
  `IllegalStateException`(unchecked)로 변환(제어된 `Map`이라 실제로는 도달 불가).

---

## 8. `sink/OpenSearchSinkFactory` — OpenSearch Sink (`K2`)

**역할**: `PageClickCount` 스트림을 OpenSearch `user-activity-agg` 인덱스에 **bulk + retry**로 적재하는
`Opensearch2Sink`를 만든다. DAG 라벨 `[sink-opensearch-agg]`.

**구성**
```java
OpensearchEmitter<PageClickCount> emitter = (pcc, ctx, indexer) ->
    indexer.add(new IndexRequest("user-activity-agg")
        .id(OpenSearchDocs.aggDocId(pcc))                              // deterministic id
        .source(OpenSearchDocs.aggDocJson(pcc), XContentType.JSON));   // JSON 본문

new Opensearch2SinkBuilder<PageClickCount>()
    .setHosts(new HttpHost(host, port, scheme))
    .setEmitter(emitter)
    .setBulkFlushMaxActions(500)        // 500건 모이면 flush
    .setBulkFlushMaxSizeMb(2)           // 또는 2MB 모이면
    .setBulkFlushInterval(2000L)        // 또는 2초마다 — 셋 중 먼저 도달하는 조건
    .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000L) // 429 등 지수 backoff 5회
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build();
```

**교과서 포인트**
- **Emitter**가 레코드 → `IndexRequest` 변환 지점. 여기서 `OpenSearchDocs`의 id·json을 끼워 넣는다.
- **Bulk**: 건수/크기/주기 셋 중 가장 먼저 충족되는 조건에 flush → 처리량 ↑. 단 sink가 느리면
  bulk 큐가 차서 **backpressure**가 상류로 전파(Web UI에서 관찰 가능 — 완료 기준의 한 항목).
- **Retry**: 일시적 오류(예: 429 too many requests)에 지수 backoff로 5회 재시도.
- **`AT_LEAST_ONCE` + deterministic id = effectively idempotent**: at-least-once라 중복 전송이
  있을 수 있지만, 같은 doc id로 덮어쓰므로 결과적으로 멱등. exactly-once 트랜잭션이 필요 없다.

---

## 9. 핵심 개념 정리 (왜 이렇게 설계했나)

1. **Event Time vs Processing Time** — 항상 이벤트에 박힌 `eventTime`(epoch ms) 기준으로 윈도잉.
   producer가 일부러 0~2분 과거로 흩뿌린 out-of-order 이벤트를, watermark(30s 허용)가
   "이 시각까지는 다 왔다"고 판단해 윈도우를 닫는다. → 재현 가능한 결과(가드레일 #1).
2. **`AggregateFunction` + `ProcessWindowFunction` 조합** — 증분 집계로 메모리를 아끼면서도
   윈도우 메타데이터(start/end)를 결과에 붙이는 표준 패턴(6·7장).
3. **멱등 sink** — OpenSearch는 트랜잭션이 아니므로 deterministic doc id 덮어쓰기로 중복을 막는다.
   재실행/장애 복구에도 문서 수가 불변(`K3`).
4. **operator name/uid 전면 부여** — Web UI 가독성 + 상태 복원 안정성(가드레일 #4).
5. **bounded 스위치** — 같은 코드로 무한 스트리밍(관찰)과 유한 배치(검증)를 전환.

---

## 10. Runtime (`R1~R3`) — Checkpoint / offset / 복구

`FlinkUserActivityAnalyticsJob`의 env 설정에 들어 있다.
- `enableCheckpointing(60s, EXACTLY_ONCE)` + timeout/min-pause/maxConcurrent(1)/tolerableFailure(3),
  `RETAIN_ON_CANCELLATION`, `file://` checkpoint storage. EXACTLY_ONCE는 Flink **내부 state** 한정이고
  OpenSearch sink는 at-least-once + deterministic id로 멱등(가드레일 #5).
- KafkaSource는 checkpoint 시 partition offset을 함께 스냅샷(`commit.offsets.on.checkpoint=true`는 가시성용).
  복구 시 offset과 window/Top N state가 함께 일관 복원된다.
- 모든 operator에 `.name()`/`.uid()` (가드레일 #4) — 복구 시 상태 매핑 키.

---

## 11. 2차 — Top N · DLQ · late · State TTL · 견고성

### 11-1. DLQ — `source/RawBytesDeserializationSchema` + `source/AvroDeserSplitter` + `model/DlqRecord`

**문제**: 1차는 source에 `forSpecific`을 직접 물려 Avro를 풀었다. 그런데 깨진 메시지(스키마 불일치 등)가
오면 `forSpecific`이 throw → 파이프라인이 죽는다(가드레일 #9). 그리고 `DeserializationSchema`는 side
output을 낼 수 없어 DLQ로 보낼 수도 없다.

**해결**: 역직렬화를 source에서 한 단계 뒤로 옮긴다.
- `RawBytesDeserializationSchema`: source는 **원본 byte[]만** 읽는다(해석 안 함).
- `AvroDeserSplitter`(`ProcessFunction<byte[], UserActivityEvent>`): `forSpecific`을 **try/catch로 감싸**
  - 성공 → main output(`UserActivityEvent`)
  - 실패 → `DlqRecord`(원본 bytes + 에러)를 `DLQ_TAG` side output으로.
- `DlqRecord`는 Avro 타입을 담지 않는(byte[]+String) 깔끔한 POJO → side output 직렬화에서 Kryo-on-Avro 회피.

> **함정(실제로 밟음)**: try/catch가 `out.collect(event)`까지 감싸면, window/sink 등 **하류**에서 난 예외를
> "역직렬화 실패"로 오인해 멀쩡한 이벤트를 DLQ로 보내고 장애도 삼켜 restart가 안 일어난다. → try/catch는
> `deserialize()`만 감싸고, `collect()`의 하류 예외는 **전파**시켜야 한다.

문서 ID는 원본 바이트 **SHA-256** → 같은 독성 메시지를 (earliest 재소비로) 다시 읽어도 같은 문서로 멱등.
테스트는 `PoisonEventProducer`(비-Avro 평문 적재) → "Magic number does not match"로 DLQ에 떨어진다.

### 11-2. Top N — `topn/TopNPagesFunction` + `model/TopPageResult`

`pageClickCounts`((7) `sink-opensearch-agg`로 가는 그 스트림)를 `keyBy(windowEnd)`로 다시 키잉해, 같은 윈도우에 속한 페이지별
카운트를 한 task로 모은다. `TopNPagesFunction`(`KeyedProcessFunction<Long, PageClickCount, TopPageResult>`):

| 콜백 | 동작 |
|---|---|
| `processElement` | `MapState(pageId→count)`에 누적, `windowEnd+1`에 이벤트타임 타이머 등록. 이미 확정(`emitted`)된 윈도우의 늦은 재발화는 무시 |
| `onTimer` | 모인 카운트를 **(count desc, pageId asc)** 정렬 → 상위 N개 `TopPageResult` emit → `emitted=true` 후 큰 state **clear**(가드레일 #6) |

- **왜 windowEnd+1 타이머?** 상류 window는 워터마크 W(≥windowEnd)를 받으면 결과 레코드들을 **먼저** 흘리고
  그 뒤 W를 전파한다. 이 operator는 레코드를 다 처리한 뒤 W로 타이머를 발화 → 해당 윈도우 전체 페이지가 모인 상태.
- **tie-break(pageId asc)** 가 중요: 같은 클릭수면 항상 같은 rank/pageId → deterministic doc id
  `windowStart_windowEnd_rank_pageId`로 멱등. (검증에서 checkout=product=66 동점 시 checkout이 rank1.)
- **bounded state + clear**: MapState는 한 윈도우의 페이지 수만큼만 쌓이고 계산 후 clear. 추가로 11-4 State TTL이
  안전망. `emitted` 플래그 덕에 late 재발화가 Top N을 흔들지 않는다(Top N은 "윈도우 종료 시점 확정" 시맨틱).

### 11-3. late event — window `allowedLateness` + late side output + `withIdleness`

- `window(...).allowedLateness(60s).sideOutputLateData(LATE_TAG)`: 윈도우 종료 후 60s까지는 늦은 이벤트로
  윈도우를 재발화(agg 갱신), 그보다 더 늦으면 `LATE_TAG` side output → `user-activity-late`(doc id=eventId).
  (Top N은 11-2의 `emitted`로 재발화 무시 → late는 agg 인덱스에만 반영.)
- **`withIdleness`**(assign-watermark): 데이터 없는 source subtask(파티션 3 < 병렬성 12)가 워터마크를 MIN으로
  묶던 R3 이월 문제를 해결 → **스트리밍에서도 윈도우가 firing**된다. (bounded는 end-of-input final 워터마크로 firing.)

### 11-4. State TTL — `StateTtlConfig`

`TopNPagesFunction`의 MapState/ValueState에 TTL(기본 1h, `OnCreateAndWrite`/`NeverReturnExpired`). 정상
흐름은 onTimer에서 clear하지만, 타이머가 끝내 발화하지 못한 windowEnd 키(이상 상황)의 state 누수를 막는 안전망.

### 11-5. 견고성 — backpressure 주입 / 장애 복구

- **backpressure(`SINK_DELAY_MS`)**: `OpenSearchSinkFactory.aggSink`의 emit마다 인위적 지연 주입. sink를 느리게
  해 backpressure를 유발한다. *다만* 이 파이프라인은 windowing이 sink 앞에서 볼륨을 크게 줄이고 agg sink가 window에
  chain돼 있어, Web UI BP 지표는 약하게 뜬다(관찰하려면 지연을 키우고 firing burst 시점을 샘플링).
- **장애 복구**: `RestartStrategyOptions` fixed-delay(3회/5s) + `FaultInjectionMapper`(`FAIL_AFTER` 전역 카운터로
  JVM당 1회 의도적 throw, `[fault-injection]`). 장애 발생 → restart strategy가 ExecutionGraph 재배포 → 마지막
  checkpoint의 offset/state(없으면 earliest)에서 재개 → 멱등 sink 덕에 정상 결과로 복구.

### 11-6. 새 sink들 — `sink/OpenSearchSinkFactory` (agg/topn/dlq/late)

네 종류 sink가 동일 bulk/retry/at-least-once 설정을 `sinkWith(...)` 헬퍼로 공유한다. 매핑/문서 ID는
`OpenSearchDocs`(agg/topn/dlq/late별 `*DocId`/`*DocJson`). 전부 deterministic id라 재실행 시 덮어쓰기(멱등).

| 인덱스 | 결과 타입 | doc id |
|---|---|---|
| `user-activity-agg` | `PageClickCount` | `pageId_windowStart_windowEnd_CLICK` |
| `user-activity-topn` | `TopPageResult` | `windowStart_windowEnd_rank_pageId` |
| `user-activity-dlq` | `DlqRecord` | 원본 bytes SHA-256 |
| `user-activity-late` | `UserActivityEvent` | eventId |

### 11-7. 2차 실행/검증 치트시트

```bash
mvn -q compile exec:java -Dexec.args=400                 # 정상 Avro 400건
mvn -q compile exec:java@poison -Dexec.args=5            # DLQ용 독성 5건
BOUNDED=true mvn -q compile exec:exec@job               # E2E (agg/topn/dlq 채움)
SINK_DELAY_MS=500 mvn -q exec:exec@job                  # backpressure 주입(스트리밍)
FAIL_AFTER=50 BOUNDED=true mvn -q compile exec:exec@job # 장애 주입→복구
EVENT_TIME_BACKDATE_MS=1200000 mvn -q compile exec:java -Dexec.args=30  # late 테스트용 과거 이벤트
```
검증 결과(클린 BOUNDED): agg 5(=윈도우 firing 5, CLICK 합계와 일치) · topn 3(rank=agg 상위3) · dlq 5(poison) · late 0.

---

### 부록 — 클래스 ↔ 구현 스텝 ↔ DAG 라벨 대응표

| 클래스 / 파일 | 스텝 | DAG operator |
|---|---|---|
| `user-activity-event.avsc` → `UserActivityEvent` | `S1` | (입력 모델) |
| `SampleEventProducer` | `S3` | (외부 producer) |
| `UserActivityKafkaSourceFactory` | `S4` | `source-user-activity-events` |
| `FlinkUserActivityAnalyticsJob` (watermark) | `S5` | `assign-watermark` |
| `FlinkUserActivityAnalyticsJob` (filter) | `P1`,`P2` | `filter-click`, keyBy |
| `PageClickCountAggregator` + `PageClickWindowResultFunction` | `P3` | `window-pageclick-5m` |
| `PageClickCount` | `P3` | (결과 모델) |
| `OpenSearchDocs` | `K1` | (문서 매핑, agg/topn/dlq/late) |
| `OpenSearchSinkFactory` | `K2`,`K3` | `sink-opensearch-agg`/`-topn`/`-dlq`/`-late` |
| `FlinkUserActivityAnalyticsJob` (env Web UI) | `W1` | (MiniCluster + UI :8082) |
| `FlinkUserActivityAnalyticsJob` (env checkpoint/restart) | `R1`~`R3` | (checkpoint 60s, restart strategy) |
| `RawBytesDeserializationSchema` + `AvroDeserSplitter` + `DlqRecord` | 2차 DLQ | `split-deser` (+DLQ side output) |
| `TopNPagesFunction` + `TopPageResult` | 2차 Top N | `topn-pages` |
| `FlinkUserActivityAnalyticsJob` (allowedLateness/late/withIdleness) | 2차 late | `window-pageclick-5m` (+late side output) |
| `FaultInjectionMapper` | 2차 견고성 | `fault-injection` (선택) |
| `PoisonEventProducer` | 2차 DLQ 테스트 | (외부 producer) |
