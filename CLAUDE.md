# CLAUDE.md — Flink 실시간 분석 파이프라인 (연습 프로젝트)

Kafka 사용자 행동 이벤트(**Avro**)를 Flink로 실시간 집계하고 OpenSearch에 저장하는 End-to-End 스트림 파이프라인.
Flink의 Event Time / Window / State / Checkpoint / Sink idempotency를 직접 구현하며 익히는 것이 목표다.
공부용 프로젝트이므로 **Source → Process → Sink를 잘게 쪼개 단계마다 검증**하며 진행한다.

## 아키텍처 (Flink DAG)

```text
Kafka(user-activity-events, Avro + Schema Registry)
  → KafkaSource                      [source-user-activity-events]
     └ Avro Deserialize (SpecificRecord, Confluent Schema Registry)
  → assignTimestampsAndWatermarks    [assign-watermark]   (Event Time, 30s out-of-orderness)
  → Filter eventType==CLICK          [filter-click]
  → keyBy(pageId)
  → 5min Tumbling Window             [window-pageclick-5m]  → PageClickCount
  → keyBy(windowEnd)                                          (2차)
  → Top N ProcessFunction           [topn-pages]            → TopPageResult (2차)
  → map: 결과 → JSON 문서            [to-os-doc]
  → OpenSearch Sink                 [sink-opensearch-agg / sink-opensearch-topn]
```

> **입력은 Avro, 결과 sink 문서는 JSON.** Avro는 Kafka source 직렬화 포맷일 뿐, OpenSearch에는 JSON 문서로 적재한다.

## 기술 스택 & 의존성

- **Java 17**, **Maven** (`maven-shade-plugin` fat-jar + `avro-maven-plugin` codegen)
- **Apache Flink 1.18.1**, **OpenSearch 2.x**, **Confluent Schema Registry**
- 핵심 의존성 (정확 버전 문자열은 Maven Central에서 확인 후 핀):
  - `org.apache.flink:flink-streaming-java:1.18.1`
  - `org.apache.flink:flink-connector-kafka:3.1.0-1.18`
  - `org.apache.flink:flink-avro:1.18.1`
  - `org.apache.flink:flink-avro-confluent-registry:1.18.1`  *(Registry 연동 deser)*
  - `org.apache.flink:flink-connector-opensearch2:2.0.0-1.18` *(OpenSearch 2.x용 — 검증 완료. `1.2.0-1.18`은 OpenSearch **1.x**용 `flink-connector-opensearch`이니 혼동 주의. opensearch2 2.0.0-1.18은 OpenSearch 클라이언트 2.13.0을 번들 → 서버 2.11.1과 2.x 호환)*
  - `org.apache.avro:avro:1.11.3` + `avro-maven-plugin` *(.avsc → SpecificRecord codegen)*
  - `com.fasterxml.jackson.core:jackson-databind:2.17.0` — **sink JSON 직렬화 전용** (입력은 Avro). ⚠️ avro 1.11.3이 jackson-core 2.14.2를 전이로 끌어와 opensearch 클라이언트(2.17.0)와 충돌하므로 **jackson-bom 2.17.0을 `dependencyManagement`로 import**해 전 모듈 버전 고정 필요.
  - 테스트: `flink-test-utils`, `flink-runtime`(MiniCluster), `junit-jupiter`
- **Confluent Maven 레포 추가** 필요: `https://packages.confluent.io/maven/` (registry client 해석용)
- Flink 런타임은 `provided` scope, 커넥터/avro/registry/jackson만 shade에 포함.

## 빌드 & 실행

```bash
mvn -q generate-sources               # .avsc → UserActivityEvent(SpecificRecord) codegen
mvn -q clean package                  # fat-jar → target/*.jar (codegen 포함)
docker compose up -d                  # Kafka + Schema Registry + OpenSearch + Dashboards
# 토픽 생성
docker compose exec kafka kafka-topics.sh --create \
  --topic user-activity-events --partitions 3 --bootstrap-server localhost:9092
# 샘플 Avro 이벤트 적재 (Avro producer 스크립트/소형 Java producer로)
# Job 제출 (로컬 클러스터) 또는 IDE에서 main 실행
flink run -c <pkg>.job.FlinkUserActivityAnalyticsJob target/*.jar
```

로컬 개발 시에는 `FlinkUserActivityAnalyticsJob`의 `main`을 IDE에서 바로 실행해도 됨(MiniCluster).

## 로컬 인프라 (docker-compose)

| 컴포넌트 | 포트 | 비고 |
|---|---|---|
| Kafka (KRaft 또는 zk) | `9092` | topic `user-activity-events`, partitions 3 |
| Schema Registry (Confluent) | `8081` | Avro 스키마 등록/호환성, subject `user-activity-events-value` |
| OpenSearch 2.x | `9200` | single-node, security 비활성(로컬 한정) |
| OpenSearch Dashboards | `5601` | 인덱스/문서 확인 |

> `docker-compose.yml`과 샘플 Avro producer는 구현 단계에서 생성한다.

## 패키지 / 클래스 구조

```text
src/main/avro/
  user-activity-event.avsc            # codegen source → UserActivityEvent
src/main/java/<group>/flink/
  model/   PageClickCount, TopPageResult         # 결과 POJO(손수 작성), OpenSearch 문서용
  source/  UserActivityKafkaSourceFactory        # KafkaSource + Avro/Registry deser
  agg/     PageClickCountAggregator, PageClickWindowResultFunction
  topn/    TopNPagesFunction                      # 2차
  sink/    OpenSearchSinkFactory, OpenSearchDocs  # 결과 POJO → JSON 문서 + doc id
  job/     FlinkUserActivityAnalyticsJob          # DAG 조립 + env 설정
target/generated-sources/avro/...                 # UserActivityEvent (빌드 산출, SpecificRecord)
```

## 도메인 모델

| 타입 | 형태 | 필드 |
|---|---|---|
| `UserActivityEvent` | **Avro SpecificRecord (codegen)** | eventId, userId, pageId, eventType, eventTime(epoch ms long), sessionId |
| `PageClickCount` | POJO | pageId, windowStart, windowEnd, count |
| `TopPageResult` | POJO | windowStart, windowEnd, rank, pageId, clickCount |

`user-activity-event.avsc` 예시:
```json
{
  "type": "record", "name": "UserActivityEvent",
  "namespace": "<group>.flink.model.avro",
  "fields": [
    {"name": "eventId",   "type": "string"},
    {"name": "userId",    "type": "string"},
    {"name": "pageId",    "type": "string"},
    {"name": "eventType", "type": "string"},
    {"name": "eventTime", "type": "long"},
    {"name": "sessionId", "type": ["null", "string"], "default": null}
  ]
}
```

## 핵심 구현 규칙 (가드레일 — 반드시 준수)

1. **Event Time 기준 처리.** Processing Time 쓰지 말 것. eventTime은 Avro `long`(epoch ms).
   ```java
   WatermarkStrategy
     .<UserActivityEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
     .withTimestampAssigner((e, ts) -> e.getEventTime());
   ```
2. **상태는 Flink State로만.** 일반 Java 필드/`static` Map에 상태 저장 **금지**.
   ```java
   // 금지: private long count;  /  private static Map<String,Long> cache;
   // 사용: ValueState / ListState / MapState / Window state (+ 필요 시 State TTL)
   ```
3. **입력 Avro / 출력 JSON 혼동 금지.** Kafka deser는 `ConfluentRegistryAvroDeserializationSchema.forSpecific(UserActivityEvent.class, schemaRegistryUrl)`. OpenSearch 문서는 결과 POJO를 JSON으로 직렬화.
4. **모든 operator에 `.name()` + `.uid()` 지정.** Web UI 병목 추적 + 재시작 시 상태 복원 안정성. 네이밍은 위 DAG의 대괄호 라벨을 따른다.
5. **OpenSearch는 트랜잭션 sink가 아니다.** exactly-once에 의존하지 말고 **deterministic document ID 기반 idempotent write**로 중복을 막는다(아래 ID 규칙).
6. **Top N에서 전체 page count를 `ListState`에 무한정 쌓지 말 것.** page 수가 많으면 state 폭증 위험 — bounded(상위 N 유지) 구조 고려, 계산 후 state clear.
7. **keyBy(pageId)** 이후 동일 pageId는 동일 task에서 처리됨을 전제로 작성.
8. Window 크기 / allowed lateness는 state size에 직접 영향 — 무분별하게 늘리지 말 것.
9. **역직렬화 실패 / 스키마 불일치** 이벤트는 파이프라인을 죽이지 말 것 — 1차는 흘려보내거나 로그, 2차는 side output/DLQ. (`forSpecific`은 실패 시 throw하므로 2차에서 try/catch 래핑 deser 고려.)

## OpenSearch 인덱스 & 문서 ID

| 결과 | 인덱스 | 문서 ID (deterministic) |
|---|---|---|
| Window 집계 | `user-activity-agg` | `pageId_windowStart_windowEnd_eventType` |
| Top N | `user-activity-topn` | `windowStart_windowEnd_rank_pageId` |

- **Bulk write** 사용, **retry 정책** 설정, sink 병목으로 인한 backpressure 가능성 고려.
- 같은 결과를 여러 번 써도 같은 ID로 덮어쓰기(upsert) → 장애 재처리 시에도 중복 문서 미발생.

## Checkpoint / State 설정

```java
env.enableCheckpointing(60_000); // 60s
```
추가로 고려: checkpoint timeout, min pause between checkpoints, tolerable checkpoint failure number,
externalized checkpoint **retain on cancellation**, state backend(메모리→RocksDB) 교체 여지.
복구 시 Kafka offset은 Flink checkpoint와 함께 관리(별도 auto-commit 의존 X).

## 구현 단계 — **1차부터, Source→Process→Sink 순서로 쪼개서**

각 스텝은 **검증 게이트**를 통과한 뒤 다음으로. 1차를 E2E로 검증한 뒤 2차를 얹는다.

### 1차 — Source
- [x] `S1` `user-activity-event.avsc` 정의 → `mvn generate-sources`로 `UserActivityEvent` codegen *(검증: `target/generated-sources`에 클래스 생성)*
- [x] `S2` docker-compose 기동(Kafka+Schema Registry+OpenSearch), 토픽 생성 *(검증: `kafka-topics --list`, registry `/subjects` 응답)*
- [x] `S3` Avro producer로 샘플 이벤트 적재 *(검증: subject `user-activity-events-value` 등록 + 토픽에 메시지)*
- [x] `S4` KafkaSource + `ConfluentRegistryAvroDeserializationSchema.forSpecific(...)` 로 읽기 → `print()` *(검증: 콘솔 출력, consumer group id 지정)*
- [x] `S5` Event Time / Watermark 부여(30s out-of-orderness) `[assign-watermark]` *(검증: probe로 부여된 event-time timestamp == eventTime 확인)*

### 1차 — Process
- [x] `P1` `eventType == CLICK` 필터 `[filter-click]`
- [x] `P2` `keyBy(pageId)`
- [x] `P3` 5분 Tumbling Window + `AggregateFunction`(count) + `ProcessWindowFunction`(windowStart/End 부착) → `PageClickCount` `[window-pageclick-5m]` *(검증: window별 count 합 == 전체 CLICK 수)*

### 1차 — Sink
- [x] `K1` `PageClickCount` → OpenSearch 문서(JSON) 매핑 + deterministic doc id
- [x] `K2` OpenSearch Sink 연결(bulk, retry) `[sink-opensearch-agg]` *(검증: `user-activity-agg` 인덱스에 문서)*
- [x] `K3` 멱등성 검증 — 동일 입력 재처리 시 문서 수 불변(덮어쓰기)

### 1차 — Web UI (로컬 관측)
- [x] `W1` 로컬 MiniCluster **Flink Web UI** 활성화 — `flink-runtime-web`(`provided`) 추가 + env를 `StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)`로 교체, `RestOptions.PORT = 8082` 지정(**8081은 Schema Registry와 충돌**). *(검증: 무한 스트리밍(`BOUNDED=false`)으로 실행 중 `http://localhost:8082`에서 DAG의 operator name/uid 라벨 · operator별 backpressure/numRecordsIn·Out · watermark · (R1 이후) checkpoint 확인. **UI는 Job 실행 중에만 생존** → `BOUNDED=true`는 즉시 종료돼 관찰 불가.)*

### 1차 — Runtime
- [ ] `R1` Checkpoint 활성화(60s) + Kafka offset checkpoint 연동
- [ ] `R2` 전 operator `.name()` + `.uid()`
- [ ] `R3` E2E 검증(produce → agg 인덱스까지 흐름)

### 2차 — Process (Top N)
- [ ] `keyBy(windowEnd)` → `TopNPagesFunction`(KeyedProcessFunction, 상위 N만 bounded 유지, 계산 후 state clear) → `TopPageResult` `[topn-pages]`
- [ ] OpenSearch `user-activity-topn` 저장 `[sink-opensearch-topn]`

### 2차 — 견고성 (DLQ / late)
- [ ] Avro 역직렬화 실패 / 스키마 불일치 → side output(DLQ)
- [ ] late event 처리(allowedLateness / lateData side output)

### 2차 — 운영
- [ ] State TTL 적용
- [ ] backpressure 테스트(sink 지연 주입) / TaskManager 장애 복구(checkpoint restore)

> Top N · DLQ · TTL을 1차에 섞지 않는다.

## 완료 기준 (동작으로 검증)

- Kafka partition ↔ Flink source parallelism 관계를 설명할 수 있는가
- Avro 이벤트가 Schema Registry 기반 SpecificRecord로 역직렬화되는가
- Event Time / Watermark가 실제로 적용되는가
- pageId별 window count가 정확한가
- (2차) Top N이 window별로 계산되는가
- (2차) 스키마 진화(필드 추가) 시 호환성 정책으로 read가 깨지지 않는가
- checkpoint 후 장애 복구가 되는가 (Kafka offset + window/Top N state 함께 복원)
- OpenSearch에 중복 문서가 생기지 않는가
- Sink 병목 시 Web UI에서 backpressure를 관찰할 수 있는가

## 관측 / 운영 메트릭

- **Flink**: numRecordsIn/Out, busyTimeMsPerSecond, backPressuredTimeMsPerSecond, idleTimeMsPerSecond, checkpoint duration/size/failure count, state size, restart count
- **Kafka**: consumer lag (전체 / partition별), records consumed rate
- **Schema Registry**: subject/version 수, 호환성 위반(register 실패) 카운트
- **OpenSearch**: indexing latency, bulk failure, rejected request, thread pool queue, heap usage, disk I/O
