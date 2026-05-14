package dev.hialt.foxwatch.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;

public class SeqClient {

    private static final String SEQ_URL =
            "http://seq-service.default.svc.cluster.local:5341/api/events/raw?clef";
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void info(String message, String... properties) {
        try {
            ObjectNode event = MAPPER.createObjectNode();
            event.put("@t", Instant.now().toString());
            event.put("@mt", message);
            event.put("@l", "Information");
            event.put("application", "foxwatch-analytics");

            for (int i = 0; i + 1 < properties.length; i += 2) {
                event.put(properties[i], properties[i + 1]);
            }

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(SEQ_URL))
                    .header("Content-Type", "application/vnd.serilog.clef")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            MAPPER.writeValueAsString(event)))
                    .build();

            HTTP.sendAsync(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception ignored) {}
    }
}