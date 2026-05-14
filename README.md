# foxwatch-analytics

A stateful Apache Flink stream processing job written in Java that performs real-time anomaly detection on IoT device telemetry published by [foxwatch](https://github.com/hialtdev/foxwatch).

[![Build](https://img.shields.io/badge/build-gradle-blue)](build.gradle.kts)

---

## Overview

foxwatch-analytics consumes the `foxwatch-telemetry` Kafka topic produced by the foxwatch Rust pipeline, applies windowed aggregations to detect device unavailability patterns, and publishes results to the `foxwatch-analytics` Kafka topic. Structured observability events are shipped directly to Seq via the CLEF HTTP API.

The job runs in **Flink Application Mode** — packaged as a fat JAR, baked into a Docker image, and managed entirely by the Flink Kubernetes Operator. No manual job submission required.

---

## Architecture

```
IoT Devices (14 sensors)
    └── Home Assistant
            └── Mosquitto MQTT Broker
                    └── foxwatch (Rust / Tokio)
                            └── foxwatch-telemetry (Kafka / Strimzi)
                                    └── foxwatch-analytics (Flink / Java)
                                            ├── ParseAndFilter      — state extraction + Unavailable filter
                                            ├── KeyedStream         — partitioned by device_id
                                            ├── TumblingWindow      — 5-minute PROCTIME windows
                                            ├── DropoutWindowFunction — counts per device per window
                                            ├── foxwatch-analytics  — Kafka sink
                                            └── Seq                 — CLEF structured events
```

foxwatch-analytics is a pure downstream consumer. The Rust ingest pipeline is unchanged — this job adds stateful analytics without touching the hot path.

---

## Technical Stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Build tool | Gradle 8 (Kotlin DSL) |
| Stream processor | Apache Flink 1.19.3 |
| Kafka connector | flink-connector-kafka 3.3.0-1.19 |
| Serialization | Jackson 2.17 |
| Observability | Direct CLEF/HTTP → Seq |
| Deployment | Flink Kubernetes Operator (Application Mode) |
| Container runtime | k3s (single-node), Docker |
| CI | Gradle shadowJar + k3s image import |

---

## Key Design Decisions

### Application Mode deployment
The job is packaged as a fat JAR via the Gradle Shadow plugin and baked into a custom Docker image. The `FlinkDeployment` CRD references the JAR directly — the operator manages the full lifecycle including restarts and upgrades. No session cluster, no manual `flink run` required.

```yaml
job:
  jarURI: local:///opt/flink/usrlib/foxwatch-analytics.jar
  entryClass: dev.hialt.foxwatch.analytics.DropoutDetectorJob
  parallelism: 1
  upgradeMode: stateless
```

### Processing time windows
The job uses `TumblingProcessingTimeWindows` rather than event time windows. Event time watermarks stall on sparse IoT data — when devices go quiet overnight the watermark freezes and windows never close. Processing time closes windows on wall-clock time every 5 minutes regardless of data flow, which is the correct tradeoff for a home fleet with intermittent traffic.

```java
.window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
```

### Direct CLEF/HTTP observability
Flink controls its own classpath and JVM startup flags, making Log4j2 appender configuration unreliable. Rather than fighting Flink's logging bootstrap, structured events are shipped directly to Seq via a lightweight `SeqClient` class using Java's built-in `HttpClient`. This mirrors the pattern used by the Rust foxwatch pipeline and eliminates the Log4j2 dependency entirely.

```java
SeqClient.info("Window closed",
    "device_id",     deviceId,
    "dropout_count", String.valueOf(count),
    "window_start",  windowStart,
    "source",        "java-dropout-detector");
```

### Keyed stream partitioning
Events are keyed by `device_id` before windowing. Each device gets its own independent state store inside Flink — a dropout cluster on `family_room_froggy` doesn't affect the window state of `kitchen` or any other device.

```java
.keyBy(t -> t.f0)
.window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
.process(new DropoutWindowFunction());
```

---

## Project Structure

```
foxwatch-analytics/
├── src/main/java/dev/hialt/foxwatch/analytics/
│   ├── DropoutDetectorJob.java   — main job: source, filter, window, sink
│   └── SeqClient.java            — async CLEF/HTTP shipper to Seq
├── k8s/
│   └── flink-application.yaml   — FlinkDeployment manifest (Application Mode)
├── Dockerfile                   — fat JAR → flink:1.19 base image
├── deploy.sh                    — build + containerize + k3s import + apply
├── build.gradle.kts             — Gradle Kotlin DSL, Shadow plugin
└── settings.gradle.kts
```

---

## Output Schema

Each closed 5-minute window emits one JSON record per device to `foxwatch-analytics`:

```json
{
  "device_id": "bedroom_socket_1",
  "dropout_count": 1,
  "window_start": "2026-05-07 00:00:00",
  "source": "java-dropout-detector"
}
```

Correlated dropouts across multiple devices in the same window indicate a WAP or upstream network event. Single-device dropouts in isolation indicate individual device instability.

---

## Observability

Seq receives structured CLEF events from the job at three points:

| Event | Properties |
|---|---|
| `Dropout detector starting up` | `source` |
| `Unavailable event received` | `device_id`, `source` |
| `Window closed` | `device_id`, `dropout_count`, `window_start`, `source` |

Filter in Seq with:
```
application = 'foxwatch-analytics'
```

---

## Deployment

### Prerequisites
- k3s cluster with Flink Kubernetes Operator installed (see foxwatch `FLINK.md`)
- Strimzi Kafka with `foxwatch-telemetry` topic
- Seq instance reachable at `seq-service.default.svc.cluster.local:5341`
- Docker + `sudo` access for k3s image import

### One-command deploy

```bash
./deploy.sh
```

This script:
1. Builds the fat JAR via `./gradlew clean shadowJar`
2. Builds the Docker image
3. Clears the k3s image cache
4. Imports the new image into k3s
5. Applies `k8s/flink-application.yaml`

### Verify

```bash
# Check pods
kubectl get pods -n flink

# Check job status
kubectl get flinkdeployment -n flink

# Stream jobmanager logs
kubectl logs -n flink -l component=jobmanager --tail=100

# Stream taskmanager logs
kubectl logs -n flink -l component=taskmanager --tail=100

# Flink dashboard
kubectl port-forward svc/foxwatch-dropout-detector-rest 8081:8081 -n flink
# Open http://localhost:8081
```

---

## Relation to Foxglove's Technical Stack

| Foxglove requirement | foxwatch-analytics implementation |
|---|---|
| Stateful stream processing over sensor data | Flink keyed streams with per-device state partitioning |
| Windowed aggregation over time-series telemetry | Tumbling PROCTIME windows, 5-minute granularity |
| Anomaly detection on device fleet | Correlated unavailability detection across device group |
| Kubernetes-native deployment | Flink Kubernetes Operator, Application Mode, CRD-managed lifecycle |
| Separation of ingest and analytic concerns | Pure Kafka consumer — Rust hot path unchanged |
| Structured observability | Direct CLEF/HTTP to Seq, device_id and dropout_count as properties |
| Polyglot systems experience | Rust ingest pipeline + Java analytics layer over shared Kafka bus |

---

## Related Projects

- [foxwatch](https://github.com/hialtdev/foxwatch) — Rust MQTT ingest pipeline that produces `foxwatch-telemetry`

---

## Author

Robert Glasser — [hialt.dev](https://hialt.dev)
