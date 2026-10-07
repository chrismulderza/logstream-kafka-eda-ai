package ai.logstream.worker.messaging;

import ai.logstream.worker.process.EventProcessor;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class MetricsConsumer {

    private static final Logger LOG = Logger.getLogger(MetricsConsumer.class);

    private final EventProcessor processor;
    private final EnrichedEventsProducer producer;

    @Inject
    public MetricsConsumer(EventProcessor processor, EnrichedEventsProducer producer) {
        this.processor = processor;
        this.producer = producer;
    }

    @Incoming("metrics")
    @Blocking // Sync inference HTTP must not run on the Vert.x event loop
    public CompletionStage<Void> consume(Message<String> message) {
        String topic = message.getMetadata(IncomingKafkaRecordMetadata.class)
                .map(IncomingKafkaRecordMetadata::getTopic)
                .orElse("unknown");
        try {
            List<Map<String, Object>> events = processor.process(message.getPayload(), topic);
            for (Map<String, Object> event : events) {
                producer.send(event);
            }
        } catch (Exception ex) {
            LOG.errorf(ex, "failed to process message from %s", topic);
        }
        return message.ack();
    }
}
