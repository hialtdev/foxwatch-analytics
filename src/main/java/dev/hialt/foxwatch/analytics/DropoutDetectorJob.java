package dev.hialt.foxwatch.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropoutDetectorJob {

    private static final Logger log = LogManager.getLogger(DropoutDetectorJob.class);
    static {
        // This is your 'Debug Emergency Flare'
        System.out.println("!!! DROPOUT DETECTOR STARTING UP - SYSTEM.OUT CHECK !!!");
        log.info("!!! DROPOUT DETECTOR STARTING UP - LOG4J CHECK !!!");
    }
    static final String KAFKA_BOOTSTRAP =
            "bitbybit-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092";
    static final String SOURCE_TOPIC = "foxwatch-telemetry";
    static final String SINK_TOPIC   = "foxwatch-analytics";
    static final String CONSUMER_GROUP = "flink-foxwatch-dropout-java";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // ── Source ────────────────────────────────────────────────────────
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "foxwatch-telemetry-source"
        );

        // ── Parse and filter Unavailable events ───────────────────────────
        DataStream<Tuple2<String, Integer>> unavailable = raw
                .map(new ParseAndFilter())
                .filter(t -> t != null);

        // ── Key by device_id, tumbling 5-minute processing time window ────
        DataStream<String> summaries = unavailable
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
                .process(new DropoutWindowFunction());

        // ── Sink ──────────────────────────────────────────────────────────
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

    // ── Parse JSON and extract device_id if state == Unavailable ─────────
    static class ParseAndFilter implements MapFunction<String, Tuple2<String, Integer>> {
        private final ObjectMapper mapper = new ObjectMapper();


        @Override
        public Tuple2<String, Integer> map(String json) {
            try {
                // Log a tiny bit more for debugging
                ObjectNode node = (ObjectNode) mapper.readTree(json);

                // Check if 'state' actually exists before asText()
                if (!node.has("state")) {
                    log.debug("Skipping message: no state field found");
                    return null;
                }

                String state = node.path("state").asText("");
                String deviceId = node.path("device_id").asText("unknown-device");

                if ("Unavailable".equalsIgnoreCase(state)) {
                    log.info("!!! DROPOUT DETECTED !!! Device: {}", deviceId);
                    return Tuple2.of(deviceId, 1);
                }
            } catch (Exception e) {
                // This will show up in the Flink TaskManager 'Stdout/Stderr' logs
                log.error("JSON Parse Failure: {} | Raw: {}", e.getMessage(), json);
            }
            return null;
        }
    }

    // ── Emit a JSON summary when each window closes ───────────────────────
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
            int count = 0;
            for (Tuple2<String, Integer> e : elements) count += e.f1;

            log.info("Window closed device_id={} dropout_count={} window_start={}",
                    deviceId, count,
                    FMT.format(Instant.ofEpochMilli(context.window().getStart())));

            ObjectNode result = mapper.createObjectNode();
            result.put("device_id",     deviceId);
            result.put("dropout_count", count);
            result.put("window_start",
                    FMT.format(Instant.ofEpochMilli(context.window().getStart())));
            result.put("source", "java-dropout-detector");

            out.collect(mapper.writeValueAsString(result));
        }
    }
}