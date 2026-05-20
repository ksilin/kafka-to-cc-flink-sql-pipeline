# kafka-to-sql-filter

Kafka-driven variant of the post-to-sql-filter prototype. Translates **Kafka subscription messages**  into Flink SQL statements on Confluent Cloud, filtering output by `vehicleId + mdc_id`, producing flat-per-signal records.

### The four topics

| Topic | Purpose | Who writes | Who reads |
|---|---|---|---|
| `kf-input-test` | signal data (raw JSON, nested `signals[]`) | You (via `produce`) | Flink filter statement |
| `kf-data-test` | Filtered flat per-signal output | Flink filter statement | You (via `observe`) |
| `kf-sub-test` | Subscription messages | You / downstream | Orchestrator (Kafka mode, V2) |
| `kf-ack-test` | ACK responses | Orchestrator (Kafka mode, V2) | downstream |

V1 file-mode doesn't use `kf-sub-test` / `kf-ack-test` for actual message flow -- subscriptions come from files, ACKs go to stdout. The topics are created for visibility and for the Quarkus Kafka-mode path.

## 0. What this project does (30-second version)

A Kafka subscription message arrives: `{vehicleId: "V1", dataIdList: ["100","200"]}`.
The orchestrator generates a Confluent Cloud Flink SQL statement that filters telemetry records by vehicleId + mdc_id, producing flat per-signal output records. Subscription updates use CC's carry-over offsets for seamless handover. Unsubscribe stops the statement. ACK sent back to Kafka.

Two orchestrator implementations exist side-by-side:
- **Plain Java** (`orchestrator/`) -- Maven, kafka-clients, synchronous, file-backed state
- **Quarkus** (`orchestrator-quarkus/`) -- Gradle, SmallRye Reactive Messaging, async Mutiny, CDI-wired

Both produce identical behavior. The Quarkus variant mirrors the patterns from the upstream service.

---

## 1. Prerequisites

```bash
# Check versions
java -version          # 17+
mvn --version          # 3.9+
python3 --version      # 3.10+
confluent --version    # v4.53+
gradle --version       # 8.3+ (only for Quarkus variant)
```

## 2. One-time setup

### 2.1 Per-machine config

```bash
cp config/env.example.sh config/env.sh
# env.sh is gitignored. Adjust CC env / cluster / pool IDs to match your environment.
```

### 2.2 Confluent Cloud login

```bash
# Interactive login:
confluent login --save

# Verify + set context (use your own IDs)
confluent environment use <your-env-id>
confluent kafka cluster use <your-cluster-id>
confluent api-key use <your-api-key> --resource <your-cluster-id>
```

### 2.3 Build both variants

```bash
# Plain Java (Maven) -- produces shaded JAR (~19MB)
( cd orchestrator && mvn package -DskipTests )
# -> target/kafka-to-sql-filter-orchestrator-0.1.0-SNAPSHOT-shaded.jar

# Quarkus (Gradle) -- produces Quarkus app
( cd orchestrator-quarkus && ./gradlew build -x test )
# -> build/quarkus-app/
```


### Step-by-step walkthrough

```bash
source config/env.sh

# 1. Clean any leftover state from previous runs
./test-data/run-live-demo.sh clean

# 2. Create all 4 topics + submit output table DDL + produce initial fixture data
./test-data/run-live-demo.sh setup

# 3. Subscribe -- creates a Flink filter statement (RUNNING)
#    Filter: vehicleId='vehicle-fixture-001' AND mdc_id IN (100, 200)
./test-data/run-live-demo.sh subscribe

# 4. Observe -- setup data is already processed (CC Flink reads from earliest)
#    Shows one-line summary per record: vehicle, mdc_id, signal name, count
#    Expect: 9 records (mdc_id 100 or 200 for vehicle-fixture-001)
./test-data/run-live-demo.sh observe

# 5. Produce more data to see additional output flow through
./test-data/run-live-demo.sh produce

# 6. Check what's running on CC
./test-data/run-live-demo.sh status

# 7. Update the filter (carry-over offsets -- seamless handover)
#    New filter: mdc_id IN (200, 300)
./test-data/run-live-demo.sh update

# 8. See the updated filter in action -- use TWO terminals:
#    Terminal 1 (start FIRST -- waits for new records only):
./test-data/run-live-demo.sh observe-new
#    Terminal 2 (produce data -- observe-new shows only these):
./test-data/run-live-demo.sh produce
#    Terminal 1 should show 6 records (mdc 200 + 300), NOT the old 9.
#    Or use 'observe' (from-beginning) to see all 9 + 6 = 15 with counts.

# 9. Unsubscribe -- stops the filter, no more output
./test-data/run-live-demo.sh unsubscribe

# 10. Verify nothing new comes out -- start observe-new, then produce:
#    Terminal 1:
./test-data/run-live-demo.sh observe-new
#    Terminal 2:
./test-data/run-live-demo.sh produce
#    Terminal 1: no new records appear (statement stopped)

# 11. Clean up when done
./test-data/run-live-demo.sh clean
```

### What to observe at each step

| After step | What you should see |
|---|---|
| `setup` | 3 topics created (input, sub, ack) + DDL creates output topic; 10 records produced to input |
| `subscribe` | ACK: `{"status":"Success","details":"subscribed",...}`; a RUNNING statement in CC |
| `observe` (right after subscribe) | 9 records already present -- CC Flink reads from earliest, so setup data is processed immediately |
| `produce` + `observe` | 18 records total (9 from setup + 9 from produce) |
| `status` | Statement name + RUNNING; all 4 topics |
| `update` | ACK: `updated`; old statement STOPPED, new one RUNNING |
| `observe-new` + `produce` (two terminals) | Terminal 1 shows 6 NEW records only (mdc 200,300) -- old 9 not mixed in |
| `observe` (single terminal) | All 15 records from beginning (9 old + 6 new) with counts |
| `unsubscribe` | ACK: `unsubscribed`; statement NOT RUNNING |
| `observe-new` + `produce` after unsub | No new records in terminal 1 (statement stopped) |

### Inspecting manually via CLI

While the demo is running (between `subscribe` and `clean`):

```bash
# List Flink statements
confluent flink statement list --cloud $CC_CLOUD --region $CC_REGION --output json \
  | jq -r '.[] | "\(.name)\t\(.status)"' | grep "kf-"

# Describe a specific statement
confluent flink statement describe kf-flt-vehiclef-1 --cloud $CC_CLOUD --region $CC_REGION

# Consume output with pretty-print
confluent kafka topic consume $KF_OUTPUT_TOPIC \
  --cluster $CC_CLUSTER_ID --from-beginning --print-key=false \
  --value-format jsonschema | grep -v '^%' | jq .

# Count records in output
confluent kafka topic consume $KF_OUTPUT_TOPIC \
  --cluster $CC_CLUSTER_ID --from-beginning --print-key=false \
  --value-format jsonschema 2>/dev/null | grep -v '^%' | wc -l

# Schema registered for output topic
confluent schema-registry schema describe --subject ${KF_OUTPUT_TOPIC}-value
```


## 3. Understand the architecture -- what lives where

### 3.1 SQL layer (shared by both variants)

```
sql/
├── 00-create-output-table.sql   <- DDL: CREATE TABLE with json-registry + id-encoding=header
├── 01-filter-template.sql       <- INSERT INTO ... SELECT with __VEHICLE_ID__, __MDC_ID_CSV__ tokens
├── 01-filter-F1.sql             <- Concrete: vehicleId='vehicle-fixture-001', mdc IN (100,200)
└── 01-filter-F1-update.sql      <- Carry-over variant: mdc IN (200,300)
```

**Open `01-filter-template.sql` and read it.** This is the core SQL pattern:

```sql
INSERT INTO __OUTPUT_TOPIC__
SELECT
    CAST(ROW(
        JSON_VALUE(CAST(val AS STRING), '$.payload.containerId'),  -- extract from raw bytes
        ...
        JSON_VALUE(signal_str, '$.mdc_id'),                        -- extract from unnested signal
        ...
    ) AS ROW<`containerId` STRING, ... `mdc_id` STRING, ...>) AS `payload`,
    ...
FROM __INPUT_TOPIC__
CROSS JOIN UNNEST(                                                 -- explode signals array
    CAST(JSON_QUERY(CAST(val AS STRING), 'lax $.payload.signals[*]'
         RETURNING ARRAY<STRING>) AS ARRAY<STRING>)
) AS T(signal_str)
WHERE JSON_VALUE(CAST(val AS STRING), '$.vehicleId') = '__VEHICLE_ID__'
  AND CAST(JSON_VALUE(signal_str, '$.mdc_id') AS BIGINT) IN (__MDC_ID_CSV__);
```

Key points:
- Input topic has NO schema -- reads raw `val` as VARBINARY, parses with `JSON_VALUE`
- `CROSS JOIN UNNEST` explodes `payload.signals[]` array -> one row per signal
- Filter: vehicleId literal + mdc_id IN clause
- Output: flat envelope with nested `payload` ROW -- all STRING fields (dodge typed-NULL bug)
- `id-encoding=header` on the DDL means output payload is clean JSON (no magic byte)

### 3.2 Plain Java orchestrator -- code flow

```
orchestrator/src/main/java/com/example/kf2sql/
```

**Follow one subscription through the code:**

**Step 1 -- Entry:** `Main.java`
- `--mode file`: reads subscription JSON from disk, calls `OrchestratorLoop.handle()`, prints ACK
- `--mode kafka`: polls subscription topic via `SubscriptionConsumer`, dispatches each, writes ACK via `AckProducer`

**Step 2 -- Parse:** `Subscription.java` (record)
- `fromJson(String)` -> Jackson deserialization
- `isUnsubscribe()` checks `dataIdList == null || empty`
- `mdcCsv()` -> `"100, 200"` for the SQL IN clause

**Step 3 -- Dispatch:** `OrchestratorLoop.java`
```java
public AckMessage handle(Subscription sub) {
    if (sub.isUnsubscribe()) return handleUnsubscribe(sub);
    if (allocator.current(sub.vehicleId()) == null) return handleSubscribe(sub);
    return handleUpdate(sub);
}
```

**Step 4a -- Subscribe:** `handleSubscribe()`
1. `allocator.next(vehicleId)` -> `"kf-flt-vehiclef-1"` (deterministic name)
2. `sqlGenerator.fromSubscription(sub, inputTopic, outputTopic)` -> concrete SQL
3. `lifecycle.submit(name, sql, null)` -> `confluent flink statement create ... --wait`
4. Returns `AckMessage.success(correlationId, "subscribed")`

**Step 4b -- Update:** `handleUpdate()`
1. `allocator.current(vehicleId)` -> previous statement name
2. `allocator.next(vehicleId)` -> new name (seq+1)
3. `sqlGenerator.fromSubscription(...)` -> new SQL with updated predicate
4. `lifecycle.submit(newName, sql, previousName)` -> create with `--property sql.tables.initial-offset-from=previousName`, NO `--wait`
5. `lifecycle.stop(previousName)` -> `confluent flink statement stop ...`
6. `lifecycle.waitForRunning(newName, timeout)` -> poll `describe` until RUNNING
7. Returns `AckMessage.success(correlationId, "updated")`

**Step 4c -- Unsubscribe:** `handleUnsubscribe()`
1. `allocator.current(vehicleId)` -> current statement name (may be null)
2. If non-null: `lifecycle.stop(name)`
3. Returns `AckMessage.success(correlationId, "unsubscribed")`

**Step 5 -- CLI wrapper:** `FlinkLifecycle.java`
- `submit()` builds: `confluent flink statement create NAME --sql SQL --compute-pool POOL --database CLUSTER --environment ENV [--wait]`
- `stop()` builds: `confluent flink statement stop NAME --cloud CLOUD --region REGION`
- `describe()` builds: `confluent flink statement describe NAME --cloud CLOUD --region REGION --output json`, parses `status` field
- Note: `create` uses `--environment`, NOT `--cloud/--region`. `stop/describe` use `--cloud/--region`.
- Note: carry-over create omits `--wait` (v2 stays PENDING until v1 stops)

**Step 6 -- Name allocator:** `StatementNameAllocator.java`
- Convention: `kf-flt-<first8alnum>-<seq>`
- Persists to JSON file (survives JVM restarts in file mode)
- Increments per vehicleId independently

### 3.3 Quarkus orchestrator -- code flow

```
orchestrator-quarkus/src/main/java/com/example/kf2sql/quarkus/
```

**Same logical flow, different wiring:**

**Step 1 -- Entry:** `SubscriptionConsumer.java`
```java
@Incoming("subscriptions")
public Uni<Void> consume(Message<Subscription> message) {
    return Uni.createFrom().item(message.getPayload())
        .onItem().ifNotNull().transformToUni(router::route)
        .eventually(() -> Uni.createFrom().completionStage(message::ack))
        ...
```
- No manual poll loop -- SmallRye handles consumer lifecycle
- `@Incoming("subscriptions")` wired to Kafka topic via `application.yaml`
- Deserialization: `SubscriptionDeserializer` (Jackson-based, configured in YAML)

**Step 2 -- Dispatch:** `SubscriptionRouter.java`
```java
public Uni<Void> route(Subscription sub) {
    return Uni.createFrom().item(() -> {
        if (sub.isUnsubscribe()) return "unsubscribe";
        return allocator.current(sub.vehicleId()) == null ? "subscribe" : "update";
    })
    .chain(action -> switch (action) {
        case "subscribe" -> subscribeHandler.handle(sub);
        case "update" -> updateHandler.handle(sub);
        case "unsubscribe" -> unsubscribeHandler.handle(sub);
    })
    .chain(ack -> Uni.createFrom().completionStage(() -> ackProducer.send(ack)))
    ...
```
- CDI-injected handlers (vs if/else in plain Java)
- Mutiny `Uni<T>` chains (vs synchronous calls)
- ACK produced via `@Channel("acks") MutinyEmitter`

**Step 3 -- Handlers:** `handler/SubscribeHandler.java`, `UpdateHandler.java`, `UnsubscribeHandler.java`
- Each is `@ApplicationScoped` CDI bean
- Each returns `Uni<AckMessage>`
- `UpdateHandler.handle()` chains: submit(no-wait) -> stop(v1) -> waitForRunning(v2) -> return ACK
- `FlinkLifecycle` methods return `Uni<>` (async ProcessBuilder via worker pool)

**Step 4 -- Config:** `application.yaml`
```yaml
mp.messaging.incoming.subscriptions:
    connector: smallrye-kafka
    topic: ${KF_SUBSCRIPTION_TOPIC:kf-sub-test}
    value.deserializer: com.example.kf2sql.quarkus.config.SubscriptionDeserializer

cc:
    compute-pool: ${CC_COMPUTE_POOL:lfcp-kknvdm}
    cluster: ${CC_CLUSTER_ID:lkc-6w3rv2}
    ...
```
- All CC parameters injected via `@ConfigProperty`
- Kafka bootstrap + SASL config via env vars
- Test profile switches to `smallrye-in-memory` connector (no real Kafka for unit tests)
