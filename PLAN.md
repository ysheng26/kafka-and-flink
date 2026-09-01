# Kafka and Flink Playground Plan

This plan builds the playground incrementally. Keep Flink running through the local `main()` function at first; only Kafka needs Docker.

## Phase 1: Verify the quickstart project

Goal: prove the generated project works before introducing Kafka.

Use:

- Java 11
- Flink 1.20.3
- IntelliJ's bundled Maven or Maven Wrapper

Run the generated `DataStreamJob.main()` from IntelliJ with a small bounded pipeline:

```text
in-memory collection -> map -> print
```

Set an explicit parallelism, such as `2`, and print the subtask index from a rich function.

Done when:

- The project compiles.
- `main()` runs from IntelliJ.
- Different subtask indexes appear in the output.

Do not add Kafka until this works.

## Phase 2: Establish a useful project structure

Keep the project small:

```text
src/main/java/.../
├── KafkaAndFlinkJob.java
├── model/
│   └── CigarettePrice.java
├── source/
│   └── CigarettePriceDeserializer.java
└── operator/
    └── LogSubtask.java
```

Because Java 11 does not support records, `CigarettePrice` can be an ordinary POJO.

Consider carrying Kafka metadata alongside the value:

```text
topic
partition
offset
key
cigarette-price value
```

The partition and offset fields make the experiments observable. `SimpleStringSchema` provides the value but not all the Kafka metadata needed for the article.

Done when:

- One class represents an input event.
- One function can log the current Flink subtask.
- The main job remains easy to read.

## Phase 3: Add the Kafka connector

For Flink 1.20, use the Kafka connector built for the 1.20 line:

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>3.4.0-1.20</version>
</dependency>
```

Keep the version distinction clear:

```text
Flink core dependencies
-> version 1.20.3
-> provided by Flink when deployed to a cluster

Kafka connector
-> version 3.4.0-1.20
-> application dependency built for Flink 1.20
```

Done when:

- Maven reloads successfully.
- IntelliJ recognizes `KafkaSource`.
- The existing non-Kafka pipeline still runs.

## Phase 4: Run Kafka with Docker Compose

Start with Kafka only. Do not add Flink containers yet.

The Compose environment needs:

```text
kafka
topic-init (optional)
```

Configure one Kafka broker in KRaft mode. Because the Flink job runs from IntelliJ on the Mac, connect to:

```text
localhost:9092
```

Create one topic:

```text
name: cigarette-prices
partitions: 3
replication factor: 1
```

An optional initialization service can wait for Kafka and create the topic automatically. Alternatively, create it manually using Kafka's CLI inside the broker container.

Done when:

- Kafka is reachable from the Mac.
- The topic exists with exactly three partitions.
- Kafka's CLI can produce and consume a test message.

## Phase 5: Create deterministic test data

Prepare enough records to make partition behavior visible. Include an explicit key:

```json
{"brand":"Marlboro","price":13.0,"country":"America"}
{"brand":"Camel","price":12.0,"country":"America"}
{"brand":"Chunghua","price":14.0,"country":"China"}
{"brand":"MildSeven","price":15.0,"country":"Japan"}
```

Possible keys:

```text
America
China
Japan
```

Records with the same key should normally go to the same Kafka partition. Do not assume that a particular country maps to a particular partition unless the producer's partition selection is controlled. Log the actual Kafka partition instead.

Done when:

- Several records exist across all three partitions.
- Each record's partition can be identified.
- At least one partition contains multiple records so ordering is visible.

## Phase 6: Build the first Kafka source

Configure a `KafkaSource` with:

```text
bootstrap servers: localhost:9092
topic: cigarette-prices
group ID: flink-playground-1
starting offsets: earliest
deserializer: metadata-preserving deserializer
```

Construct this pipeline:

```text
KafkaSource
    -> log Kafka partition, offset, and Flink subtask
    -> print
```

Give important operators names. Stable UIDs become useful when restoring state later:

```text
Kafka Source
Inspect Assignment
Print
```

Use a new group ID or reset progress between experiments when a clean run is required.

Done when every printed record includes information such as:

```text
kafka-partition=2
kafka-offset=7
flink-subtask=1
brand=MildSeven
```

## Phase 7: Run the partition and source-parallelism experiments

Keep the Kafka topic at three partitions and change only the Kafka source parallelism.

| Experiment | Kafka partitions | Source parallelism | Expected observation |
|---|---:|---:|---|
| A | 3 | 1 | One source subtask reads all three partitions |
| B | 3 | 2 | One subtask reads two partitions and one reads another |
| C | 3 | 3 | Each active subtask can receive one partition |
| D | 3 | 4 | One source subtask has no Kafka partition to read |

Do not depend on a specific partition-to-subtask mapping unless Flink documents it as stable. The important observation is how many partitions each subtask receives.

For every experiment, record:

```text
source parallelism
Kafka partition
Flink source subtask
records observed
```

These results can become tables or diagrams in the blog post.

## Phase 8: Separate source and operator parallelism

Add a downstream operator and configure its parallelism independently:

```text
Kafka source, parallelism 2
    -> translation map, parallelism 4
    -> print, parallelism 1
```

Log the subtask index in both the source inspection and translation operators. This demonstrates:

```text
Kafka partitions constrain useful source parallelism

but

downstream operators can have their own parallelism
```

Also observe operator chaining. Compatible operators may run in the same task. Observe the default first; disable or break chaining later only if it makes a particular experiment clearer.

Done when the output distinguishes:

```text
Kafka partition
source subtask
downstream operator subtask
```

## Phase 9: Explore data redistribution

Treat each operation as a separate experiment.

### `union`

Create two compatible streams and union them:

```text
Kafka source A --\
                 +--> union --> inspect
Kafka source B --/
```

Observe that `union` combines streams of the same type. It does not group records by key or provide two separately addressable inputs.

### `connect`

Create two different stream types:

```text
price stream -------\
                     +--> connect --> CoProcessFunction
translation stream -/
```

Use this to demonstrate that `connect` preserves two logical inputs, allowing each side to be handled differently.

### `keyBy`

Apply:

```text
keyBy(country)
```

Then log the downstream subtask. Verify:

- All records for the same country reach the same keyed subtask.
- Different countries may reach different subtasks.
- `keyBy` redistributes records independently of their original Kafka partitions.

The important distinction is:

```text
Kafka partitioning
-> determines source-side distribution

Flink keyBy
-> determines keyed distribution downstream
```

Keep `union`, `connect`, and `keyBy` as separate jobs or scenarios initially.

## Phase 10: Add checkpointing

After the routing experiments work, enable periodic checkpointing.

Start with:

```text
checkpoint interval: 5-10 seconds
restart strategy: fixed delay
stateful operator: count records per country
```

The pipeline can become:

```text
Kafka source
    -> keyBy(country)
    -> stateful count
    -> print
```

Log:

```text
Kafka partition and offset
country
count before update
count after update
Flink subtask
```

Allow at least one checkpoint to complete before injecting a failure.

For a local demonstration, throw an exception from an operator after a chosen record. Make sure the failure happens only once; otherwise, restoring the task may cause the same condition to fail repeatedly. A JVM-static flag is acceptable for this deliberately local demonstration, but it is not a production technique.

Observe:

- The task fails.
- The local Flink runtime restarts it.
- Managed counts return to their checkpointed values.
- Kafka source positions return to the same consistent checkpoint.
- Records processed after the checkpoint may be processed again.

This experiment supports the claim that Flink stores Kafka positions with checkpointed state.

## Phase 11: Decide whether a Flink cluster is needed

Continue using local `main()` execution for:

- Kafka partition assignment
- Source parallelism
- Operator parallelism
- `union`, `connect`, and `keyBy`
- Task-level failures
- Basic checkpoint restoration

Add a Dockerized Flink cluster only to demonstrate:

- Submitting a packaged JAR
- Task slots in the Web UI
- Multiple TaskManagers
- Killing an entire TaskManager
- Persistent checkpoints across process loss
- Savepoints
- Rescaling a deployed job

The cluster is an optional later phase, not a prerequisite for the core article.

## Phase 12: Add automated tests last

Separate tests into two categories.

Pure unit tests:

```text
JSON deserialization
translation logic
key extraction
business transformations
```

Integration tests:

```text
Kafka topic creation
KafkaSource consumption
checkpoint and restart behavior
```

Do not begin with Testcontainers or a Flink test cluster. First make each scenario understandable when run manually, and then automate the scenarios worth preserving.

## Suggested implementation order

```text
1. Generated job runs
2. Local parallel subtasks are visible
3. Kafka Compose starts
4. Three-partition topic exists
5. KafkaSource reads records
6. Partition and subtask metadata are printed
7. Run source parallelism 1, 2, 3, and 4
8. Add downstream parallelism
9. Demonstrate keyBy
10. Demonstrate union and connect
11. Add checkpointing and an injected failure
12. Consider a Flink Docker cluster
```

Keep every phase runnable before beginning the next one. The first major milestone is one output line that shows both a Kafka partition and a Flink subtask.
