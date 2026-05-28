# foxwatch-analytics — Codebase Reference

> README.md covers architecture, deployment, and design decisions well.
> This document covers implementation details not visible from the README.
> Optimized for hialt-recall RAG retrieval.

---

## What does foxwatch-analytics do?

foxwatch-analytics is a Java Flink job that consumes the `foxwatch-telemetry` Kafka topic,
counts device Unavailable transitions in 5-minute tumbling processing-time windows, and
publishes dropout summaries to the `foxwatch-analytics` Kafka topic. It also ships
structured CLEF logs to Seq. It is a pure downstream consumer — it does not modify
the foxwatch Rust pipeline in any way.

---

## What Kafka topics does foxwatch-analytics use?

| Topic | Direction | Role |
|---|---|---|
| `foxwatch-telemetry` | Source (reads) | Raw IoT telemetry from foxwatch Rust pipeline |
| `foxwatch-analytics` | Sink (writes) | Dropout summary records, one per device per window |

Kafka bootstrap server: `bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092`

Flink consumer group: `flink-foxwatch-dropout-java`

This consumer group is distinct from the SQL-based consumer group `flink-foxwatch-production`
used in the foxwatch Flink SQL jobs. Both consume `foxwatch-telemetry` independently.

---

## What is the output record schema?

One JSON record per device per closed 5-minute window, written to `foxwatch-analytics`:

```json
{
  "device_id":     "bedroom_socket_1",
  "dropout_count": 1,
  "window_start":  "2026-05-07 00:00:00",
  "source":        "java-dropout-detector"
}
```

`window_start` is UTC, formatted as `yyyy-MM-dd HH:mm:ss`.
`source` is always the string `"java-dropout-detector"`.

---

## How does ParseAndFilter work? (the filter logic is more complex than it looks)

`ParseAndFilter` is a `RichFlatMapFunction` with per-device keyed state. It does not
simply filter for `state == "Unavailable"`. It implements a state machine:

### State machine

Each device has a `ValueState<String>` named `lastKnownNonUnavailableState` that tracks
the last known On or Off state. Initial value is null (no state seen yet).

Rules:
1. If the message has no `state` field or it is null — skip entirely
2. If `state` is `"On"` or `"Off"` — update the keyed state, do not emit
3. If the event is Unavailable AND last known state was `"On"` — emit `(device_id, 1)` to the window
4. If the event is Unavailable AND last known state was `"Off"` — suppress, log to Seq as "Unavailable suppressed"
5. If the event is Unavailable AND last known state is null (no prior state seen) — suppress silently

### Why suppress Off→Unavailable?

A device going Unavailable after being Off is expected behavior (device powered down).
Only On→Unavailable transitions represent genuine unexpected dropouts worth counting.

### is_available field duck-typing

The filter checks for an optional `is_available` boolean field in the JSON envelope:

```java
boolean unavailable =
    (node.has("is_available") && !node.get("is_available").isNull())
        ? !node.path("is_available").asBoolean(true)
        : "Unavailable".equalsIgnoreCase(state);
```

If `is_available` is present and non-null, it takes precedence over the `state` string.
This is a forward-compatibility hook — the current foxwatch Rust pipeline does not emit
`is_available`, so all current messages fall through to the `state` string comparison.

---

## What keyed state does Flink maintain?

One `ValueState<String>` per device key: `lastKnownNonUnavailableState`

Stores `"On"` or `"Off"` (or null if no non-Unavailable event seen yet for that device).
Updated on every On or Off message. Read on every Unavailable message to decide whether
to emit or suppress. State is partitioned by `device_id` — each device's state is
independent.

State backend: default (hashmap, in-memory). No RocksDB, no checkpointing configured.
If the job restarts, all per-device state is lost and devices start with null state again.
`upgradeMode: stateless` in the FlinkDeployment manifest reflects this.

---

## Source file responsibilities

| File | What it does |
|---|---|
| `DropoutDetectorJob.java` | main(), Kafka source/sink wiring, ParseAndFilter, DropoutWindowFunction |
| `SeqClient.java` | Static CLEF/HTTP shipper to Seq using Java HttpClient, fire-and-forget async |

Both classes are in package `dev.hialt.foxwatch.analytics`.

---

## DropoutDetectorJob pipeline stages

```
KafkaSource (foxwatch-telemetry)
    └── keyBy(device_id)           — extract device_id from JSON for pre-filter keying
            └── flatMap(ParseAndFilter)  — state machine filter, emits Tuple2(device_id, 1)
                    └── keyBy(t.f0)     — re-key by device_id for windowing
                            └── TumblingProcessingTimeWindows(5 min)
                                    └── process(DropoutWindowFunction)  — count and serialize
                                            └── KafkaSink (foxwatch-analytics)
```

Note: there are two `keyBy` calls. The first is before `ParseAndFilter` to enable keyed
state in the flatMap. The second is after filtering to partition the windowed aggregation.

---

## SeqClient implementation details

`SeqClient` uses `java.net.http.HttpClient` (Java 11+ built-in) — no external HTTP library.
All calls are fire-and-forget via `sendAsync()`. Exceptions are silently swallowed.
The `HttpClient` is a static singleton.

Seq endpoint: `http://seq-service.default.svc.cluster.local:5341/api/events/raw?clef`

Content-Type: `application/vnd.serilog.clef`

Every event includes these fields automatically:
- `@t` — current UTC instant (ISO-8601)
- `@mt` — message template string
- `@l` — always `"Information"`
- `application` — always `"foxwatch-analytics"`

Additional properties are passed as varargs string pairs: `key, value, key, value, ...`

To filter foxwatch-analytics events in Seq: `application = 'foxwatch-analytics'`

---

## Seq events emitted

| Event message | Additional properties |
|---|---|
| `"Dropout detector starting up"` | `source` |
| `"Unavailable event received"` | `device_id`, `source` |
| `"Unavailable suppressed"` | `device_id`, `suppressed_reason` (`"preceded_by_off"`), `source` |
| `"Window closed"` | `device_id`, `dropout_count`, `window_start`, `source` |
| `"JSON parse failure"` | `error`, `source` |

---

## Kubernetes deployment details

FlinkDeployment name: `foxwatch-dropout-detector` (namespace: `flink`)

Flink dashboard port-forward: `kubectl port-forward svc/foxwatch-dropout-detector-rest 8081:8081 -n flink`

Image: `hialtdev/foxwatch-analytics:latest` with `imagePullPolicy: Never` (loaded into k3s locally)

JAR location inside image: `/opt/flink/usrlib/foxwatch-analytics.jar`

Entry class: `dev.hialt.foxwatch.analytics.DropoutDetectorJob`

Resources: JobManager 1536m RAM / 0.5 CPU, TaskManager 1024m RAM / 0.5 CPU, 2 task slots, parallelism 1

`upgradeMode: stateless` — no checkpoint state is preserved on restart or upgrade.

---

## Build and deploy

```bash
./deploy.sh
```

deploy.sh steps:
1. `./gradlew clean shadowJar` — builds fat JAR to `build/libs/foxwatch-analytics-1.0.0.jar`
2. `docker build` — bakes JAR into `flink:1.19` base image
3. Clears k3s image cache
4. `docker save | sudo k3s ctr images import -` — loads image into k3s
5. `kubectl apply -f k8s/flink-application.yaml` — applies FlinkDeployment CRD

---

## Build configuration

Build tool: Gradle 8 with Kotlin DSL (`build.gradle.kts`)
Java source/target compatibility: Java 11 (compiled with Java 21 toolchain)
Fat JAR: Gradle Shadow plugin 8.1.1, classifier stripped so output is `foxwatch-analytics-1.0.0.jar`

Flink core (`flink-streaming-java`, `flink-clients`) is `compileOnly` — provided by the
Flink cluster at runtime, not bundled into the fat JAR.

Bundled into fat JAR (implementation scope):
- `flink-connector-kafka:3.3.0-1.19`
- `flink-json:1.19.3`
- `jackson-databind:2.17.0`

---

## How does this relate to the foxwatch Flink SQL jobs?

foxwatch (the Rust project) also has Flink SQL jobs in `flink/jobs/` that read
`foxwatch-telemetry` and write `foxwatch-analytics`. foxwatch-analytics (this project)
does the same thing in Java with the DataStream API.

Key differences:

| Aspect | foxwatch SQL jobs | foxwatch-analytics (this project) |
|---|---|---|
| Language | Flink SQL | Java DataStream API |
| Deployment | Session cluster, manual submit | Application Mode, operator-managed |
| State | Stateless SQL aggregation | Keyed ValueState per device |
| Filter logic | Simple WHERE state = 'Unavailable' | State machine: On→Unavailable only |
| Consumer group | `flink-foxwatch-production` | `flink-foxwatch-dropout-java` |
| Suppression | None | Suppresses Off→Unavailable transitions |
| is_available | Not supported | Forward-compatible duck-typing |

Both consume `foxwatch-telemetry` independently and both write to `foxwatch-analytics`.
The Java job's dropout counts will differ from the SQL job's counts because of the
On→Unavailable filtering logic.

---

## Does foxwatch-analytics use a database?

No. It reads from Kafka and writes to Kafka only. No MongoDB, no PostgreSQL, no filesystem
persistence. All state is in-memory Flink keyed state and is lost on restart.
