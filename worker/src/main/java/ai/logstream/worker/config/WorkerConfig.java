package ai.logstream.worker.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.Optional;

@ConfigMapping(prefix = "worker")
public interface WorkerConfig {

    @WithName("window-seconds")
    @WithDefault("900")
    double windowSeconds();

    @WithName("min-samples")
    @WithDefault("4")
    int minSamples();

    @WithName("tte-alert-threshold-seconds")
    @WithDefault("3600")
    double tteAlertThresholdSeconds();

    @WithName("alert-cooldown-seconds")
    @WithDefault("300")
    double alertCooldownSeconds();

    @WithName("classify-logs")
    @WithDefault("false")
    boolean classifyLogs();

    @WithName("llm-on-metric-alerts")
    @WithDefault("true")
    boolean llmOnMetricAlerts();

    Inference inference();

    interface Inference {
        /**
         * OpenAI-compatible base URL including {@code /v1}. Leave unset / empty to disable LLM.
         * Prefer env {@code WORKER_INFERENCE_BASE_URL} or {@code INFERENCE_BASE_URL} (see application.properties).
         * Must be {@link Optional}: SmallRye rejects empty-string defaults for plain {@link String}.
         */
        @WithName("base-url")
        Optional<String> baseUrl();

        @WithName("api-key")
        Optional<String> apiKey();

        @WithDefault("default")
        String model();

        @WithName("timeout-seconds")
        @WithDefault("15")
        double timeoutSeconds();

        @WithName("max-tokens")
        @WithDefault("400")
        int maxTokens();

        default boolean enabled() {
            return baseUrl().filter(url -> !url.isBlank()).isPresent();
        }

        default String normalizedBaseUrl() {
            String url = baseUrl().orElse("").trim();
            while (url.endsWith("/")) {
                url = url.substring(0, url.length() - 1);
            }
            return url;
        }
    }
}
