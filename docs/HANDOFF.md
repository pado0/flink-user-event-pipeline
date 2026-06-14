# HANDOFF — Flink 실시간 분석 파이프라인 (연습 프로젝트)

> 작업 인수인계 문서. 현재까지의 구현 상태 · 검증 결과 · 다음 할 일 · 환경 제약을 정리한다.
> 최종 갱신: **2026-06-13** (K1·K2·K3 OpenSearch sink 완료) / 기준 커밋: `0f8609e` (+ K1·K2·K3 미커밋 작업트리)

---

## 1. 한 줄 요약

Kafka(Avro) 사용자 행동 이벤트를 Flink로 실시간 집계해 OpenSearch에 적재하는 E2E 스트림 파이프라인.
**전체 스펙·가드레일은 [`CLAUDE.md`](../CLAUDE.md)가 단일 출처(SSOT)** — 이 문서는 진행 상황 스냅샷이다.

## 2. 진행 상황 (CLAUDE.md 체크리스트 기준)

| 단계 | 내용 | 상태 |
|---|---|---|
| **S1** | `user-activity-event.avsc` 정의 → `UserActivityEvent` codegen | ✅ 완료 |
| **S2** | docker-compose 기동(Kafka+SR+OpenSearch+Dashboards) + 토픽 생성 | ✅ 완료 |
| **S3** | Avro producer로 샘플 이벤트 적재 | ✅ 완료 |
| **S4** | KafkaSource + Confluent Avro deser → `print()` | ✅ 완료 |
| **S5** | Event Time / Watermark 부여(30s out-of-orderness) | ✅ 완료 |
| **P1·P2·P3** | filter-click → keyBy(pageId) → 5min tumbling window → `PageClickCount` | ✅ 완료 |
| **K1·K2·K3** | `PageClickCount` → JSON 문서(deterministic id) → OpenSearch sink(bulk/retry) + 멱등성 검증 | ✅ 완료 |
| **R1** | Checkpoint(60s) 활성화 + Kafka offset checkpoint 연동 | ⬜ **다음 차례** |
| R2·R3 | 전 operator name/uid 점검 / E2E 검증 | ⬜ |
| 2차 | Top N / DLQ / State TTL / backpressure | ⬜ |

> **작업 규칙(중요):** 사용자는 **한 스텝씩만** 구현하고 멈추길 원함. 검증 게이트 통과 후 다음 진행. 임의로 다음 스텝 선행 금지.

## 3. 지금까지 만든 것

```text
filnk-practice/
├── CLAUDE.md                          # 프로젝트 스펙·가드레일 (SSOT)
├── docker-compose.yml                 # S2: Kafka(KRaft)+SR+OpenSearch 2.11.1+Dashboards
├── pom.xml                            # S1 codegen + S3 producer + S4 Flink 의존성, exec plugin(producer/job)
├── .gitignore                        # target/, .idea/, *.iml, .DS_Store, *.log
├── docs/
│   └── HANDOFF.md                     # (이 문서)
└── src/main/
    ├── avro/
    │   └── user-activity-event.avsc   # codegen source → UserActivityEvent
    └── java/com/example/flink/
        ├── producer/
        │   └── SampleEventProducer.java            # S3: 샘플 Avro 이벤트 적재 producer
        ├── source/
        │   └── UserActivityKafkaSourceFactory.java # S4: KafkaSource + Confluent Avro registry deser
        ├── model/
        │   └── PageClickCount.java                 # P3: 윈도우 집계 결과 POJO (sink 문서 원본)
        ├── agg/
        │   ├── PageClickCountAggregator.java       # P3: 증분 count AggregateFunction
        │   └── PageClickWindowResultFunction.java  # P3: 윈도우 메타 부착 ProcessWindowFunction
        ├── sink/
        │   ├── OpenSearchDocs.java                 # K1: PageClickCount → JSON 문서(Jackson) + deterministic doc id
        │   └── OpenSearchSinkFactory.java          # K2: Opensearch2Sink(bulk/retry, at-least-once)
        └── job/
            └── FlinkUserActivityAnalyticsJob.java  # DAG 조립(현재 source→assign-watermark→filter-click→keyBy→window→sink-opensearch-agg)
```

빌드 산출물 `target/generated-sources/avro/.../UserActivityEvent.java`는 **gitignore 대상**(커밋 안 함).

### S1 — Avro 스키마 + codegen
- `src/main/avro/user-activity-event.avsc`: record `UserActivityEvent`, namespace `com.example.flink.model.avro`
  - 필드: `eventId`(string), `userId`(string), `pageId`(string), `eventType`(string), `eventTime`(long, epoch ms), `sessionId`(["null","string"] default null)
- `pom.xml`: groupId `com.example`, artifactId `flink-user-activity-analytics`, Java 17, **avro 1.11.3 + avro-maven-plugin만** 포함. `stringType=String` (CharSequence 아님). Flink/Kafka/OpenSearch/Jackson/shade는 이후 스텝에서 추가 예정.

### S2 — 로컬 인프라
- `docker-compose.yml` 4개 서비스:
  - **kafka** `confluentinc/cp-kafka:7.6.1` — KRaft 단일노드, host `localhost:9092` / 내부 `kafka:29092`, auto-create-topics **false**, named volume `kafka-data`, healthcheck(kafka-topics --list)
  - **schema-registry** `confluentinc/cp-schema-registry:7.6.1` — `localhost:8081`, kafka healthy 후 기동
  - **opensearch** `opensearchproject/opensearch:2.11.1` — single-node, security 비활성, heap 512m, named volume `opensearch-data`, `9200`/`9600`
  - **opensearch-dashboards** `2.11.1` — `localhost:5601`
- 토픽 `user-activity-events` (partitions **3**, RF 1) 생성 — named volume로 재시작에도 보존됨.

### S3 — 샘플 Avro 이벤트 적재 producer
- `src/main/java/com/example/flink/producer/SampleEventProducer.java`: 소형 Kafka producer.
  - `KafkaAvroSerializer`(value) + `StringSerializer`(key), `schema.registry.url=http://localhost:8081`, `acks=all`.
  - 첫 직렬화 시 스키마가 subject **`user-activity-events-value`**(TopicNameStrategy)로 **자동 등록**됨.
  - key = `pageId` → 동일 pageId는 동일 파티션 (이후 `keyBy(pageId)` 일관성 학습용).
  - 데이터: pageId 5종 / userId 5종 / eventType `CLICK`:`VIEW`=3:1, `eventTime`=현재−(0~120s) 흩뿌림, `sessionId`는 절반만 채움(null 케이스 포함).
  - 이 producer는 **Flink DAG와 무관한 외부 유틸리티** (입력 Avro를 만들어 주는 도구).
- `pom.xml`: `kafka-clients:3.6.1`, `io.confluent:kafka-avro-serializer:7.6.1`, Confluent 레포, `exec-maven-plugin`(mainClass 지정) 추가.
- ⚠️ slf4j 바인딩은 일부러 미포함(NOP) → 실행 시 `SLF4J: ... NOP logger` 경고는 정상. producer는 자체 `System.out`으로 결과 출력.

### S4 — Flink KafkaSource + Avro registry deser → print()
- `source/UserActivityKafkaSourceFactory.java`: `KafkaSource<UserActivityEvent>` 빌더.
  - deser = `ConfluentRegistryAvroDeserializationSchema.forSpecific(UserActivityEvent.class, registryUrl)`
    (메시지의 schema id로 Registry에서 writer 스키마 조회 → SpecificRecord 디코드).
  - `setGroupId("flink-user-activity-analytics")`(검증 요건), `setStartingOffsets(earliest)`.
  - `bounded` 플래그: true면 `setBounded(latest)` → 시작 시점까지만 읽고 종료(검증/배치용), 기본 false=무한 스트리밍.
- `job/FlinkUserActivityAnalyticsJob.java`: `env.fromSource(source, noWatermarks, "source-user-activity-events")` → `.print()`.
  - 가드레일대로 operator에 `.name()`+`.uid()` 부여(`source-user-activity-events`, `print-events`). Watermark는 S5에서.
  - env override: `BOOTSTRAP_SERVERS` / `SCHEMA_REGISTRY_URL` / `KAFKA_GROUP_ID` / `BOUNDED`.
- `pom.xml`: `flink-streaming-java`·`flink-clients`·`flink-connector-base`(**provided**), `flink-connector-kafka:3.1.0-1.18`·`flink-avro`·`flink-avro-confluent-registry`(**compile**, 이후 shade 대상).
  - ⚠️ **실행 함정(해결됨)**: Flink는 provided라 (1) `exec`는 `classpathScope=compile`로 provided까지 포함해야 하고, (2) `flink-connector-kafka`의 provided 전이의존이 누락되어 `flink-connector-base`를 명시 추가했으며, (3) `exec:java`(동일 JVM)는 MiniCluster classloader 문제로 `ExecutionConfig ClassNotFound`가 나므로 **Flink Job은 `exec:exec`(forked JVM)** 로 실행한다. → producer=`exec:java`, job=`exec:exec@job`.

### S5 — Event Time / Watermark 부여
- `job/FlinkUserActivityAnalyticsJob.java`만 수정(다른 파일·pom 변경 없음). DAG: `source-user-activity-events → assign-watermark → watermark-probe → print-events`.
  - source 뒤에 `assignTimestampsAndWatermarks(...)` operator를 **분리**(name/uid=`assign-watermark`) — CLAUDE.md DAG가 `[assign-watermark]`를 별도 노드로 두므로. source의 `fromSource(...)`에는 계속 `WatermarkStrategy.noWatermarks()`.
  - 전략: `forBoundedOutOfOrderness(Duration.ofSeconds(30)).withTimestampAssigner((e,ts) -> e.getEventTime())` — 가드레일대로 Event Time = Avro `eventTime`(epoch ms).
  - 검증용 `WatermarkProbe`(name/uid=`watermark-probe`): non-keyed `ProcessFunction<UserActivityEvent,String>`, 상태 없는 pass-through 진단기. 각 레코드의 `ctx.timestamp()`(부여된 event-time)·`ctx.timerService().currentWatermark()`·pageId/eventType를 한 줄 출력 → `print()`. S4의 `.print()` 자리를 대체하는 임시 스캐폴딩(P3 window가 붙으면 제거 예정).
  - ⚠️ event-time **타이머는 keyed stream에서만** 가능 → keyBy(P2) 전이라 probe에서 타이머로 watermark firing은 검증 불가. 실제 firing 검증은 P3 window에서.

### P1·P2·P3 — filter-click → keyBy(pageId) → 5분 tumbling window (한 번에 구현)
세 스텝을 한 묶음으로 구현. S5의 검증용 `watermark-probe`/`print-events`는 제거하고 윈도우 파이프라인으로 대체했다.
DAG: `… → assign-watermark → filter-click → keyBy(pageId) → window-pageclick-5m → print-window-result`.

- **P1 [filter-click]** (`job` 내 `.filter`): `eventType == "CLICK"`만 통과. operator name/uid=`filter-click`. VIEW 등 비-클릭 이벤트는 여기서 탈락.
- **P2 keyBy(pageId)** (`job` 내 `.keyBy(UserActivityEvent::getPageId)`): pageId별 파티셔닝 → 동일 pageId는 동일 task에서 집계(가드레일 #7). keyBy는 partitioning이라 operator name/uid 미부여.
- **P3 [window-pageclick-5m]**: `TumblingEventTimeWindows.of(Time.minutes(5))` + `aggregate(aggFn, windowFn)`.
  - `model/PageClickCount.java`: 결과 POJO(`pageId`, `windowStart`, `windowEnd`, `count`). Flink POJO 규칙(무인자 ctor + private 필드 + getter/setter) 준수 → 효율적 직렬화. sink 문서(JSON) 원본.
  - `agg/PageClickCountAggregator.java`: `AggregateFunction<UserActivityEvent, Long, Long>` — **증분** count(원본 이벤트 미적재, 누적 Long 1개만 상태로). `merge`도 구현(session window 대비).
  - `agg/PageClickWindowResultFunction.java`: `ProcessWindowFunction<Long, PageClickCount, String, TimeWindow>` — 증분 결과(카운트 1건)에 window 메타(start/end)와 key(pageId)를 부착해 `PageClickCount` 방출. (레거시 `WindowFunction`의 현대적 대체 API)
  - operator name/uid=`window-pageclick-5m`. 출력은 임시로 `.print()`(name/uid=`print-window-result`) — K1/K2에서 OpenSearch sink로 교체 예정.
- ⚠️ **증분 집계(aggregate)** 선택 이유: `ProcessWindowFunction` 단독은 윈도우의 전 이벤트를 state에 버퍼링하지만, `AggregateFunction`과 결합하면 누적값만 유지해 state 폭증을 막는다(가드레일 #2·#8).
- ⚠️ **윈도우 firing 타이밍**: bounded 모드는 end-of-input에서 final watermark(MAX) 방출로 전 윈도우 즉시 firing(검증 편함). streaming 모드는 `watermark > windowEnd`일 때(= windowEnd + 30s out-of-orderness 경과 시) firing.
- ⚠️ 윈도우 경계는 **epoch 기준 정렬**(00/05/10분…), 이벤트의 첫 도착 시각이 아님.

### K1·K2·K3 — OpenSearch sink (JSON 문서 + deterministic id + bulk/retry + 멱등성, 한 번에 구현)
세 스텝을 한 묶음으로 구현. P3의 `print-window-result`를 OpenSearch sink로 대체했다.
DAG: `… → window-pageclick-5m → sink-opensearch-agg`.

- **K1 [매핑]** `sink/OpenSearchDocs.java`: `PageClickCount` → OpenSearch 문서.
  - **문서 ID(deterministic)** = `pageId_windowStart_windowEnd_CLICK`. eventType은 filter-click(P1)을 거쳐 항상 `CLICK`이라 상수로 박음. CLAUDE.md "OpenSearch 인덱스 & 문서 ID" 표의 `pageId_windowStart_windowEnd_eventType` 규칙.
  - **문서 본문(JSON)** = Jackson `ObjectMapper.writeValueAsBytes(Map)`. 필드: `pageId`, `eventType`, `windowStart`/`windowEnd`(epoch ms), `windowStartIso`/`windowEndIso`(Dashboards용 ISO-8601 보조), `count`. 가드레일 #3(입력 Avro / **출력 JSON**) 준수 — Jackson은 sink 직렬화 전용.
  - ⚠️ `emit`이 checked 예외를 못 던지므로 `JsonProcessingException`은 `OpenSearchDocs` 내부에서 `IllegalStateException`으로 감쌈(제어된 Map이라 실질 도달 불가).
- **K2 [sink]** `sink/OpenSearchSinkFactory.java`: `Opensearch2Sink<PageClickCount>` (통합 `Sink` API, `DataStream.sinkTo(...)`).
  - emitter: `new IndexRequest(AGG_INDEX).id(docId).source(jsonBytes, XContentType.JSON)` → `indexer.add(...)`.
  - **Bulk**: `setBulkFlushMaxActions(500)` / `setBulkFlushMaxSizeMb(2)` / `setBulkFlushInterval(2000ms)` (먼저 도달하는 조건에서 flush). **Retry**: `setBulkFlushBackoffStrategy(EXPONENTIAL, 5, 1000ms)`.
  - `setDeliveryGuarantee(AT_LEAST_ONCE)` + deterministic id = **effectively idempotent**. (OpenSearch는 트랜잭션 sink가 아님 — 가드레일 #5)
  - operator name/uid = `sink-opensearch-agg`.
- **K3 [멱등성]**: 동일 입력 재처리 시 같은 doc id로 덮어쓰기(upsert) → 문서 수 불변. (검증은 §4 참조)
- **DAG/job**: `job/FlinkUserActivityAnalyticsJob.java`에서 `print()` → `.sinkTo(OpenSearchSinkFactory.aggSink(host, port, scheme))`. env override 추가: `OPENSEARCH_HOST`(localhost) / `OPENSEARCH_PORT`(9200) / `OPENSEARCH_SCHEME`(http).
- ⚠️ **의존성 정정/함정** (`pom.xml`):
  - CLAUDE.md에 적혀 있던 `flink-connector-opensearch2:1.2.0-1.18`은 **존재하지 않음**(`1.2.0-1.18`은 OpenSearch **1.x**용 `flink-connector-opensearch`의 버전). OpenSearch 2.x용 정확한 좌표는 **`flink-connector-opensearch2:2.0.0-1.18`**. (CLAUDE.md도 정정함)
  - 이 커넥터는 OpenSearch 클라이언트 **2.13.0**을 번들 → 서버 2.11.1과 2.x 호환(정상 동작 확인).
  - **jackson 충돌**: avro 1.11.3이 `jackson-core:2.14.2`를 전이로 끌어와, opensearch 클라이언트가 쓰는 jackson(2.17.0, `StreamConstraintsException`은 2.15+)과 충돌 → 런타임 `NoClassDefFoundError`. **`dependencyManagement`로 `jackson-bom:2.17.0`을 import**해 전 jackson 모듈을 2.17.0으로 고정(depth 무관 override)하여 해결. `jackson-databind`는 compile로 명시.
  - opensearch2 커넥터/jackson은 **compile**(이후 shade 대상) — Flink dist에 없으므로 provided 아님. exec:exec@job(classpathScope=compile)에 자동 포함.

## 4. 재현 — 어떻게 띄우고 검증하나

> ⚠️ 이 머신은 **colima** 런타임. `docker compose`(공백)가 아니라 **`docker-compose`(하이픈)** 사용. (자세한 환경 제약은 §6)

```bash
# (A) 인프라 기동
docker-compose up -d

# (B) 토픽 생성 (최초 1회 — 이미 생성됨, 보존됨)
docker-compose exec kafka kafka-topics --create \
  --topic user-activity-events --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# (C) Avro codegen 확인 (S1)
mvn -q generate-sources
ls target/generated-sources/avro/com/example/flink/model/avro/UserActivityEvent.java

# (D) 샘플 이벤트 적재 (S3) — compile 시 codegen도 함께 수행됨
mvn -q compile exec:java                  # 기본 60건
mvn -q compile exec:java -Dexec.args=200  # 건수 지정

# (E) Flink Job 실행 — forked JVM. consumer group id 지정됨.
#     현재 DAG = filter→keyBy→5min window→OpenSearch sink(user-activity-agg)
BOUNDED=true mvn -q compile exec:exec@job # 시작 시점까지 읽고 종료(검증용) → OpenSearch 적재 후 종료
mvn -q compile exec:exec@job              # 무한 스트리밍(실제 파이프라인). Ctrl+C로 종료
# env override: OPENSEARCH_HOST(localhost) / OPENSEARCH_PORT(9200) / OPENSEARCH_SCHEME(http)
```

### P3 윈도우 집계 정합성 검증 커맨드
```bash
# 1) bounded 실행해서 윈도우 결과만 추출
BOUNDED=true mvn -q compile exec:exec@job 2>&1 | grep PageClickCount

# 2) 윈도우 카운트 합 (= 전체 CLICK 수와 일치해야 함: 필터+텀블링은 손실/중복 없이 CLICK을 분할)
BOUNDED=true mvn -q compile exec:exec@job 2>&1 | grep PageClickCount \
  | grep -oE 'count=[0-9]+' | grep -oE '[0-9]+' | paste -sd+ - | bc

# 3) 토픽 전체 eventType 분포 (CLICK 수 직접 확인 → 위 합과 대조)
docker-compose exec -T schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --topic user-activity-events \
  --property schema.registry.url=http://schema-registry:8081 \
  --from-beginning --max-messages <총건수> --timeout-ms 20000 2>/dev/null \
  | grep -oE '"eventType":"[A-Z]+"' | sort | uniq -c
```

### K1·K2·K3 OpenSearch sink 검증 커맨드
```bash
# 0) sink 실행 후 인덱스 refresh (검색 가시화)
curl -s -XPOST localhost:9200/user-activity-agg/_refresh

# 1) 문서 수 (= window 결과 행 수, P3 검증의 24행과 일치해야 함)
curl -s localhost:9200/user-activity-agg/_count

# 2) count 합계(= 전체 CLICK 수 255) + pageId 분포
curl -s localhost:9200/user-activity-agg/_search -H 'Content-Type: application/json' -d '{
  "size":0,
  "aggs":{"total_clicks":{"sum":{"field":"count"}},
          "by_page":{"terms":{"field":"pageId.keyword","size":10}}}}'

# 3) 샘플 문서 1건 — _id = pageId_windowStart_windowEnd_CLICK 형식 확인
curl -s 'localhost:9200/user-activity-agg/_search?size=1'

# 4) K3 멱등성: BOUNDED job을 2회 실행 → 문서 수 불변(중복 X), _version만 증가(덮어쓰기)
curl -s 'localhost:9200/user-activity-agg/_doc/<docId>' | grep -oE '"_version":[0-9]+'
```

### S3 적재 내용 확인 커맨드
```bash
# 1) subject 등록 확인 → ["user-activity-events-value"]
curl -s localhost:8081/subjects

# 2) 등록된 스키마(버전 1) 확인
curl -s localhost:8081/subjects/user-activity-events-value/versions/1

# 3) 파티션별 메시지 건수(=offset 합) 확인
docker-compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic user-activity-events

# 4) 실제 메시지 내용을 Avro로 디코드해서 보기 (앞 5건)
docker-compose exec -T schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --topic user-activity-events \
  --property schema.registry.url=http://schema-registry:8081 \
  --from-beginning --max-messages 5 --timeout-ms 15000 2>/dev/null
```

### 검증 결과 (통과 ✔)
| 항목 | 명령 | 기대값 |
|---|---|---|
| codegen | `ls target/.../UserActivityEvent.java` | 클래스 생성, `getEventTime()`→long, 나머지→String |
| 토픽 | `docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092` | `user-activity-events` (Partitions 3) |
| Schema Registry (S3 후) | `curl -s localhost:8081/subjects` | `["user-activity-events-value"]` (S3 적재로 등록됨) |
| 토픽 메시지 (S3 후) | `kafka.tools.GetOffsetShell ... --topic user-activity-events` | offset 합 = 적재 건수 (예: 60) |
| Flink S4 read | `BOUNDED=true mvn -q compile exec:exec@job` | exit 0, `UserActivityEvent` JSON이 적재 건수만큼 stdout 출력 |
| Flink S5 watermark | `BOUNDED=true mvn -q compile exec:exec@job` | exit 0, `[probe]` 라인마다 **recordTs == eventTime** (timestamp assigner 동작) |
| Flink P1·P2·P3 window | `BOUNDED=true mvn -q compile exec:exec@job` | exit 0, `PageClickCount{...}` 행, **카운트 합 == 전체 CLICK 수** |
| Flink K1·K2 sink | `BOUNDED=true … ; curl …/user-activity-agg/_count` | exit 0, 문서 24건, **count 합 == 255(전체 CLICK)**, `_id`=deterministic |
| Flink K3 멱등성 | BOUNDED job 2회 실행 후 `_count`/`_version` | 문서 수 **24 불변**, `_version` 1→2(덮어쓰기), count 동일 |
| OpenSearch | `curl -s localhost:9200/_cluster/health` | `status: green`, nodes 1 |
| Dashboards | `curl -s -o /dev/null -w '%{http_code}' localhost:5601/api/status` | `200` |

> S3 실측: 60건 적재 → subject `user-activity-events-value`(version 1, id 1) 등록, 파티션 분포 p0=0 / p1=35 / p2=25 (key=pageId 해시로 5개 키가 2개 파티션에 분산), avro-console-consumer로 CLICK/VIEW·sessionId(null 포함) 정상 디코드.
>
> S4 실측: `BOUNDED=true` 실행으로 60건이 SpecificRecord로 역직렬화되어 print 출력(`9>`/`10>` prefix=source 병렬도), eventType 47 CLICK/13 VIEW, sessionId null·값 모두 정상. exit 0.
>
> S5 실측: `BOUNDED=true` 실행 → exit 0, probe 60건, **recordTs == eventTime 60/60 일치**(awk 파싱 확인). watermark는 bounded/버스트 입력이라 초기 레코드 전부 `MIN(uninitialized)`로 표시 — 주기적(기본 200ms) watermark 방출 전에 60건이 한 번에 통과하고 end-of-input에서 final MAX가 방출되기 때문(정상). watermark가 실제로 "진행"하는 모습은 P3 window/타이머에서 확인 예정.
>
> P1·P2·P3 실측: 토픽 총 320건(=기존 120 + 신규 200) 대상 `BOUNDED=true` 실행 → exit 0. 윈도우 결과 24행 출력, pageId 5종(search/product/cart/home/checkout)이 5분 윈도우별로 분리 집계됨. **윈도우 카운트 합 255 == 토픽 전체 CLICK 255건**(avro-console-consumer 분포: CLICK 255 / VIEW 65)으로 정확히 일치 → 필터(P1)·keyBy(P2)·텀블링 윈도우(P3)가 손실/중복 없이 동작. VIEW 65건은 filter-click에서 전량 제외 확인.
>
> K1·K2·K3 실측: 토픽 320건 대상 `BOUNDED=true` 실행 → exit 0(BUILD SUCCESS). `user-activity-agg` 인덱스에 **문서 24건**(P3 윈도우 24행과 일치), **count 합계 255.0 == 전체 CLICK 255**, pageId 분포 page-cart/checkout/product/search 각 5 + page-home 4 = 24. 샘플 `_id`=`page-product_1781323500000_1781323800000_CLICK`(deterministic 규칙 정확), `_source`에 pageId/eventType/window(start·end·Iso)/count 필드 정상. **K3 멱등성**: 동일 입력으로 job 재실행 → 문서 수 **24 그대로**(중복 0), 동일 doc의 `_version` 1→2(덮어쓰기 발생), `count`=11 동일, 합계 255 동일 → deterministic id 기반 upsert로 멱등 확인.

## 5. 다음 할 일 — R1 (Runtime: Checkpoint)

CLAUDE.md `R1`: **Checkpoint 활성화(60s) + Kafka offset checkpoint 연동.**
- `job`의 env 설정에 `env.enableCheckpointing(60_000)` 추가. 추가 고려(CLAUDE.md "Checkpoint / State 설정"): checkpoint timeout, min pause between checkpoints, tolerable checkpoint failure number, externalized checkpoint **retain on cancellation**, state backend(메모리→RocksDB) 교체 여지.
- Kafka offset은 Flink checkpoint와 함께 관리(별도 auto-commit 의존 X) — KafkaSource는 checkpoint 시 offset을 함께 스냅샷.
- 이후 `R2`(전 operator name/uid 점검 — 대부분 부여됨, 누락 확인), `R3`(E2E 검증: produce → agg 인덱스).
- ⚠️ **사용자 요청 전까지 선행 구현 금지.** 한 스텝씩 검증 게이트 통과 후 진행.

> 참고: AT_LEAST_ONCE sink는 checkpoint 시점에 pending bulk를 flush한다. 현재는 checkpoint 미활성이라 bulk interval(2s)/close flush로 적재되며, R1에서 checkpoint를 켜면 sink flush가 checkpoint와 정렬된다(멱등 id 덕에 정확성은 이미 보장).

## 6. 환경 제약 (Apple Silicon + colima) — 반드시 숙지

> 메모리 [[local-infra-env]] 와 동일 내용. 이 제약들을 모르면 인프라 기동에서 막힌다.

1. **`docker compose`(v2 플러그인) 없음 → `docker-compose`(하이픈, standalone v2.32.0)만 동작.** CLAUDE.md 빌드 가이드는 `docker compose`(공백)로 적혀 있으니 치환 필요.
2. **OpenSearch는 2.11.1로 핀.** 2.12+ 이미지의 번들 JDK21이 Apple Silicon Docker에서 `SIGILL`(java.lang.System.registerNatives) 크래시(exit 134). 2.11.1은 JDK17 번들이라 정상.
3. **colima 메모리 ≥ 약 6~8GB 필요.** 기본 2GB에선 OpenSearch가 OOM kill(exit 137). 현재 사용자가 **7.7GB**로 상향해 green 기동 확인. colima 메모리 변경 시 colima 재시작 → 전 컨테이너 SIGTERM(143) → `docker-compose up -d` 재기동 필요.
4. Confluent 이미지라 토픽 CLI는 **`kafka-topics`** (`.sh` 없음). CLAUDE.md는 `kafka-topics.sh`로 적혀 있음.
5. kafka/opensearch는 named volume(`kafka-data`, `opensearch-data`) → 재시작해도 토픽/데이터 보존.

## 7. Git

- 원격: `git@github.com:pado0/flink-user-event-pipeline.git` (SSH), `origin/main` 추적 설정 완료.
- 커밋 이력:
  - `672140f` `chore: 프로젝트 스캐폴딩 (S1 Avro 스키마 + S2 로컬 인프라)` — 푸시 완료
  - `03ea38b` `feat: S3 Avro 샘플 이벤트 producer 적재`
  - `7339e36` `feat: S4 KafkaSource + Confluent Avro registry deser → print()`
  - `d4ea7af` `feat: S5 Event Time / Watermark 부여 + handoff 문서 갱신`
  - `6258919` `feat: P1·P2·P3 filter-click → keyBy(pageId) → 5min tumbling window`
  - `0f8609e` `docs: HANDOFF Git 섹션을 origin 동기화 상태로 정정` ← 현재 HEAD(푸시됨)
- **K1·K2·K3는 아직 미커밋**(작업트리). 변경: `pom.xml`(opensearch2 2.0.0-1.18 + jackson-bom + jackson-databind), `job/FlinkUserActivityAnalyticsJob.java`(sink 연결), 신규 `sink/OpenSearchDocs.java`·`sink/OpenSearchSinkFactory.java`, `CLAUDE.md`/`docs/HANDOFF.md` 갱신. 커밋 메시지(안): `feat: K1·K2·K3 OpenSearch sink(JSON 문서 + deterministic id + bulk/retry + 멱등)`.

## 8. 참고

- 단일 출처 스펙: [`CLAUDE.md`](../CLAUDE.md) — DAG, 가드레일, OpenSearch 인덱스/문서 ID 규칙, 체크리스트 전문.
- 관련 메모리: `project-progress`, `local-infra-env` (`~/.claude/.../memory/`).
