₩₩# HANDOFF — Flink 실시간 분석 파이프라인 (연습 프로젝트)

> 작업 인수인계 문서. 현재까지의 구현 상태 · 검증 결과 · 다음 할 일 · 환경 제약을 정리한다.
> 최종 갱신: **2026-06-12** (S3 완료) / 기준 커밋: `672140f` (+ S3 미커밋 작업트리)

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
| **S4** | KafkaSource + Confluent Avro deser → `print()` | ⬜ **다음 차례** |
| S5~ | Watermark / Filter / Window / Sink / Checkpoint | ⬜ |
| 2차 | Top N / DLQ / State TTL / backpressure | ⬜ |

> **작업 규칙(중요):** 사용자는 **한 스텝씩만** 구현하고 멈추길 원함. 검증 게이트 통과 후 다음 진행. 임의로 다음 스텝 선행 금지.

## 3. 지금까지 만든 것

```text
filnk-practice/
├── CLAUDE.md                          # 프로젝트 스펙·가드레일 (SSOT)
├── docker-compose.yml                 # S2: Kafka(KRaft)+SR+OpenSearch 2.11.1+Dashboards
├── pom.xml                            # S1 avro codegen + S3 producer 의존성(kafka-clients, kafka-avro-serializer, exec plugin)
├── .gitignore                        # target/, .idea/, *.iml, .DS_Store, *.log
├── docs/
│   └── HANDOFF.md                     # (이 문서)
└── src/main/
    ├── avro/
    │   └── user-activity-event.avsc   # codegen source → UserActivityEvent
    └── java/com/example/flink/producer/
        └── SampleEventProducer.java   # S3: 샘플 Avro 이벤트 적재 producer
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
| OpenSearch | `curl -s localhost:9200/_cluster/health` | `status: green`, nodes 1 |
| Dashboards | `curl -s -o /dev/null -w '%{http_code}' localhost:5601/api/status` | `200` |

> S3 실측: 60건 적재 → subject `user-activity-events-value`(version 1, id 1) 등록, 파티션 분포 p0=0 / p1=35 / p2=25 (key=pageId 해시로 5개 키가 2개 파티션에 분산), avro-console-consumer로 CLICK/VIEW·sessionId(null 포함) 정상 디코드.

## 5. 다음 할 일 — S4

CLAUDE.md `S4`: **KafkaSource + `ConfluentRegistryAvroDeserializationSchema.forSpecific(UserActivityEvent.class, ...)` 로 읽기 → `print()`.**
- 검증 게이트: 콘솔에 이벤트 출력, consumer group id 지정.
- 필요 작업(예상): `pom.xml`에 Flink 런타임(`flink-streaming-java`, `provided`) + `flink-connector-kafka` + `flink-avro-confluent-registry` 추가, `source/UserActivityKafkaSourceFactory` 작성, 간단한 `main`에서 `env.fromSource(...).print()`.
- ⚠️ **사용자 요청 전까지 선행 구현 금지.** (한 스텝씩)

## 6. 환경 제약 (Apple Silicon + colima) — 반드시 숙지

> 메모리 [[local-infra-env]] 와 동일 내용. 이 제약들을 모르면 인프라 기동에서 막힌다.

1. **`docker compose`(v2 플러그인) 없음 → `docker-compose`(하이픈, standalone v2.32.0)만 동작.** CLAUDE.md 빌드 가이드는 `docker compose`(공백)로 적혀 있으니 치환 필요.
2. **OpenSearch는 2.11.1로 핀.** 2.12+ 이미지의 번들 JDK21이 Apple Silicon Docker에서 `SIGILL`(java.lang.System.registerNatives) 크래시(exit 134). 2.11.1은 JDK17 번들이라 정상.
3. **colima 메모리 ≥ 약 6~8GB 필요.** 기본 2GB에선 OpenSearch가 OOM kill(exit 137). 현재 사용자가 **7.7GB**로 상향해 green 기동 확인. colima 메모리 변경 시 colima 재시작 → 전 컨테이너 SIGTERM(143) → `docker-compose up -d` 재기동 필요.
4. Confluent 이미지라 토픽 CLI는 **`kafka-topics`** (`.sh` 없음). CLAUDE.md는 `kafka-topics.sh`로 적혀 있음.
5. kafka/opensearch는 named volume(`kafka-data`, `opensearch-data`) → 재시작해도 토픽/데이터 보존.

## 7. Git

- 원격: `git@github.com:pado0/flink-user-event-pipeline.git` (SSH), `origin/main` 추적 설정 완료.
- 커밋 `672140f` `chore: 프로젝트 스캐폴딩 (S1 Avro 스키마 + S2 로컬 인프라)` 푸시 완료.
- **S3 작업은 아직 미커밋** (작업트리): `pom.xml`(producer 의존성), `src/.../SampleEventProducer.java`, `CLAUDE.md`/`HANDOFF.md` 문서 갱신. → 다음 커밋 대상.

## 8. 참고

- 단일 출처 스펙: [`CLAUDE.md`](../CLAUDE.md) — DAG, 가드레일, OpenSearch 인덱스/문서 ID 규칙, 체크리스트 전문.
- 관련 메모리: `project-progress`, `local-infra-env` (`~/.claude/.../memory/`).
