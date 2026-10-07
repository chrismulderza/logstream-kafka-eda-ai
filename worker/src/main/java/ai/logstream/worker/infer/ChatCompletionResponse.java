package ai.logstream.worker.infer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ChatCompletionResponse(List<Choice> choices) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Choice(Message message) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Message(String content) {
    }

    public String firstContent() {
        if (choices == null || choices.isEmpty() || choices.getFirst().message() == null) {
            return "";
        }
        String content = choices.getFirst().message().content();
        return content == null ? "" : content;
    }
}
