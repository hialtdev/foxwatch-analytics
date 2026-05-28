package dev.hialt.foxwatch.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DropoutDetectorJob {

    static final String KAFKA_BOOTSTRAP =
            "bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092";
    static final String SOURCE_TOPIC  = "foxwatch-telemetry";
    static final String SINK_TOPIC    = "foxwatch-analytics";
    static final String CONSUMER_GROUP = "flink-foxwatch-dropout-java";

    public static void main(String[] args) throws Exception {

        SeqClient.info("Dropout detector starting up",
                "source", "java-dropout-detector");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "foxwatch-telemetry-source"
        );

        DataStream<Tuple2<String, Integer>> unavailable = raw
                .keyBy(json -> {
                    try {
                        ObjectNode node = (ObjectNode) new ObjectMapper().readTree(json);
                        return node.path("device_id").asText("unknown-device");
                    } catch (Exception e) {
                        return "unknown-device";
                    }
                })
                .flatMap(new ParseAndFilter());

        DataStream<String> summaries = unavailable
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .process(new DropoutWindowFunction());

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(SINK_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        summaries.sinkTo(sink);

        env.execute("foxwatch-dropout-detector");
    }

    // ── Parse and filter Unavailable events ──────────────────────────────
    // Keyed state remembers last known non-Unavailable state per device.
    static class ParseAndFilter extends RichFlatMapFunction<String, Tuple2<String, Integer>> {
        private final ObjectMapper mapper = new ObjectMapper();
        private transient ValueState<String> lastKnownNonUnavailableState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<String> desc =
                    new ValueStateDescriptor<>("lastKnownNonUnavailableState", String.class);
            lastKnownNonUnavailableState = getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(String json, Collector<Tuple2<String, Integer>> out) {
            try {
                ObjectNode node = (ObjectNode) mapper.readTree(json);

                if (!node.has("state") || node.get("state").isNull()) {
                    return;
                }

                String state    = node.path("state").asText("");
                String deviceId = node.path("device_id").asText("unknown-device");

                // Prefer explicit availability (new foxwatch envelope field),
                // fall back to legacy interpretation via `state`.
                boolean unavailable =
                        (node.has("is_available") && !node.get("is_available").isNull())
                                ? !node.path("is_available").asBoolean(true)
                                : "Unavailable".equalsIgnoreCase(state);

                if (!unavailable) {
                    // Update state on every On/Off event.
                    if ("On".equalsIgnoreCase(state) || "Off".equalsIgnoreCase(state)) {
                        lastKnownNonUnavailableState.update(
                                "On".equalsIgnoreCase(state) ? "On" : "Off"
                        );
                    }
                    return;
                }

                // Unavailable received: only count if preceded by a known "On".
                String last = lastKnownNonUnavailableState.value();
                if ("On".equalsIgnoreCase(last)) {
                    SeqClient.info("Unavailable event received",
                            "device_id", deviceId,
                            "source", "java-dropout-detector");
                    out.collect(Tuple2.of(deviceId, 1));
                    return;
                }

                if ("Off".equalsIgnoreCase(last)) {
                    SeqClient.info("Unavailable suppressed",
                            "device_id", deviceId,
                            "suppressed_reason", "preceded_by_off",
                            "source", "java-dropout-detector");
                    return;
                }
            } catch (Exception e) {
                SeqClient.info("JSON parse failure",
                        "error", e.getMessage(),
                        "source", "java-dropout-detector");
            }
        }
    }

    // ── Emit summary when window closes ──────────────────────────────────
    static class DropoutWindowFunction
            extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {

        private final ObjectMapper mapper = new ObjectMapper();
        private static final DateTimeFormatter FMT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withZone(ZoneOffset.UTC);

        @Override
        public void process(
                String deviceId,
                Context context,
                Iterable<Tuple2<String, Integer>> elements,
                Collector<String> out
        ) throws Exception {
            SeqClient.info("Window process called",
                    "device_id", deviceId,
                    "source", "java-dropout-detector");
            int count = 0;
            for (Tuple2<String, Integer> e : elements) count += e.f1;

            String windowStart = FMT.format(
                    Instant.ofEpochMilli(context.window().getStart()));

            SeqClient.info("Window closed",
                    "device_id",     deviceId,
                    "dropout_count", String.valueOf(count),
                    "window_start",  windowStart,
                    "source",        "java-dropout-detector");

            ObjectNode result = mapper.createObjectNode();
            result.put("device_id",     deviceId);
            result.put("dropout_count", count);
            result.put("window_start",  windowStart);
            result.put("source",        "java-dropout-detector");

            out.collect(mapper.writeValueAsString(result));
        }
    }
}