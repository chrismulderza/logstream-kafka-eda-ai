package ai.logstream.worker.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import java.util.Map;

@ApplicationScoped
public class EnrichedEventsProducer {

    private static final Logger LOG = Logger.getLogger(EnrichedEventsProducer.class);

    private final Emitter<Record<String, String>> emitter;
    private final ObjectMapper objectMapper;

    @Inject
    public EnrichedEventsProducer(
            @Channel("enriched") Emitter<Record<String, String>> emitter,
            ObjectMapper objectMapper) {
        this.emitter = emitter;
        this.objectMapper = objectMapper;
    }

    public void send(Map<String, Object> event) {
        try {
            String host = event.get("host") == null ? "" : String.valueOf(event.get("host"));
            String json = objectMapper.writeValueAsString(event);
            emitter.send(Record.of(host, json));
            LOG.infof(
                    "emitted %s host=%s instance=%s tte=%s",
                    firstNonNull(event.get("alert_type"), event.get("event_type")),
                    event.get("host"),
                    event.get("instance"),
                    event.get("tte_seconds"));
        } catch (Exception ex) {
            LOG.error("failed to emit enriched event", ex);
        }
    }

    private static Object firstNonNull(Object a, Object b) {
        return a != null ? a : b;
    }
}
