package ai.logstream.worker.infer;

import ai.logstream.worker.config.WorkerConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class InferenceClient {

    private static final Logger LOG = Logger.getLogger(InferenceClient.class);

    private final WorkerConfig config;
    private final ObjectMapper objectMapper;
    private volatile InferenceRestClient restClient;

    @Inject
    public InferenceClient(WorkerConfig config, ObjectMapper objectMapper) {
        this.config = config;
        this.objectMapper = objectMapper;
    }

    public boolean enabled() {
        return config.inference().enabled();
    }

    public Map<String, Object> classifyJson(List<ChatMessage> messages) {
        if (!enabled()) {
            return Map.of();
        }
        try {
            InferenceRestClient client = client();
            ChatCompletionRequest request = new ChatCompletionRequest(
                    config.inference().model(),
                    0,
                    config.inference().maxTokens(),
                    messages
            );
            ChatCompletionResponse response = client.complete(request);
            return parseJsonObject(response.firstContent());
        } catch (Exception ex) {
            LOG.error("Inference request failed; continuing without LLM fields", ex);
            return Map.of();
        }
    }

    private InferenceRestClient client() {
        InferenceRestClient existing = restClient;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (restClient != null) {
                return restClient;
            }
            var builder = QuarkusRestClientBuilder.newBuilder()
                    .baseUri(URI.create(config.inference().normalizedBaseUrl()))
                    .readTimeout((long) (config.inference().timeoutSeconds() * 1000), TimeUnit.MILLISECONDS)
                    .connectTimeout((long) (config.inference().timeoutSeconds() * 1000), TimeUnit.MILLISECONDS);
            config.inference().apiKey()
                    .filter(key -> !key.isBlank())
                    .ifPresent(key -> builder.register(new BearerAuthFilter(key)));
            restClient = builder.build(InferenceRestClient.class);
            return restClient;
        }
    }

    Map<String, Object> parseJsonObject(String content) {
        String text = content == null ? "" : content.strip();
        if (text.isEmpty()) {
            return Map.of();
        }
        try {
            Map<String, Object> data = objectMapper.readValue(text, new TypeReference<>() {
            });
            return data == null ? Map.of() : data;
        } catch (Exception ignored) {
            int start = text.indexOf('{');
            int end = text.lastIndexOf('}');
            if (start >= 0 && end > start) {
                try {
                    Map<String, Object> data = objectMapper.readValue(
                            text.substring(start, end + 1), new TypeReference<>() {
                            });
                    return data == null ? Map.of() : data;
                } catch (Exception ex) {
                    return Map.of("summary", text);
                }
            }
            return Map.of("summary", text);
        }
    }
}
