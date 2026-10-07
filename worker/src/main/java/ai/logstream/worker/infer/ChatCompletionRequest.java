package ai.logstream.worker.infer;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record ChatCompletionRequest(
        String model,
        double temperature,
        @JsonProperty("max_tokens") int maxTokens,
        List<ChatMessage> messages
) {
}
