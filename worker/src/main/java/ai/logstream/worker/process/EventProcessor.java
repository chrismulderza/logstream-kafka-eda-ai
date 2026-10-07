package ai.logstream.worker.process;

import ai.logstream.worker.config.WorkerConfig;
import ai.logstream.worker.infer.InferenceClient;
import ai.logstream.worker.infer.Prompts;
import ai.logstream.worker.parse.MetricsParser;
import ai.logstream.worker.parse.MetricsParser.DiskSample;
import ai.logstream.worker.parse.MetricsParser.LogEvent;
import ai.logstream.worker.predict.ExhaustionEstimate;
import ai.logstream.worker.predict.RollingWindowStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class EventProcessor {

    private static final Logger LOG = Logger.getLogger(EventProcessor.class);

    private final WorkerConfig config;
    private final InferenceClient inference;
    private final ObjectMapper objectMapper;
    private final RollingWindowStore windows;
    private final ConcurrentHashMap<String, Double> lastAlert = new ConcurrentHashMap<>();

    @Inject
    public EventProcessor(WorkerConfig config, InferenceClient inference, ObjectMapper objectMapper) {
        this.config = config;
        this.inference = inference;
        this.objectMapper = objectMapper;
        this.windows = new RollingWindowStore(config.windowSeconds(), config.minSamples());
    }

    public List<Map<String, Object>> process(String raw, String topic) {
        Map<String, Object> payload = decode(raw);
        if (payload == null) {
            return List.of();
        }
        double now = System.currentTimeMillis() / 1000.0;
        double ts = MetricsParser.parseTimestamp(payload, now);
        List<Map<String, Object>> events = new ArrayList<>();

        if (config.classifyLogs()) {
            MetricsParser.extractLog(payload).ifPresent(logEvent -> {
                Map<String, Object> classified = classifyLog(logEvent, topic);
                if (classified != null) {
                    events.add(classified);
                }
            });
        }

        for (DiskSample sample : MetricsParser.extractDiskSamples(payload, ts)) {
            windows.add(sample.host(), sample.instance(), sample.timestamp(), sample.used(), sample.capacity());
            Map<String, Object> alert = maybeStorageAlert(sample, topic);
            if (alert != null) {
                events.add(alert);
            }
        }
        return events;
    }

    private Map<String, Object> classifyLog(LogEvent logEvent, String topic) {
        if (!inference.enabled()) {
            return null;
        }
        Map<String, Object> rca = inference.classifyJson(Prompts.rcaMessages(
                logEvent.host(),
                logEvent.syslogtag(),
                logEvent.facility(),
                logEvent.severity(),
                logEvent.message()));
        Map<String, Object> severity = inference.classifyJson(Prompts.severityMessages(
                logEvent.host(),
                "topic=" + topic + " syslogtag=" + logEvent.syslogtag(),
                logEvent.message()));
        if (rca.isEmpty() && severity.isEmpty()) {
            return null;
        }
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("@timestamp", MetricsParser.utcNowIso());
        event.put("event_type", "LOG_CLASSIFICATION");
        event.put("host", logEvent.host());
        event.put("source_topic", topic);
        event.put("syslogtag", logEvent.syslogtag());
        event.put("facility", logEvent.facility());
        event.put("reported_severity", logEvent.severity());
        event.put("message", logEvent.message());
        String sev = stringOrEmpty(severity.get("severity")).toUpperCase();
        event.put("severity", sev.isEmpty() ? null : sev);
        event.put("severity_score", asScore(severity.get("score")));
        event.put("severity_rationale", severity.get("rationale"));
        event.put("root_cause", rca.get("root_cause"));
        event.put("subsystem", rca.get("subsystem"));
        event.put("summary", rca.get("summary"));
        event.put("recommended_action", rca.get("recommended_action"));
        event.put("worker", "predictive-ai-worker");
        return event;
    }

    private Map<String, Object> maybeStorageAlert(DiskSample sample, String topic) {
        ExhaustionEstimate estimate = windows.estimate(sample.host(), sample.instance());
        if (estimate == null) {
            return null;
        }
        if (estimate.tteSeconds() > config.tteAlertThresholdSeconds()) {
            return null;
        }
        String key = sample.host() + "\0" + sample.instance();
        double now = System.currentTimeMillis() / 1000.0;
        Double last = lastAlert.get(key);
        if (last != null && now - last < config.alertCooldownSeconds()) {
            return null;
        }
        lastAlert.put(key, now);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("@timestamp", MetricsParser.utcNowIso());
        event.put("event_type", "PREDICTIVE_ALERT");
        event.put("alert_type", RollingWindowStore.ALERT_TYPE);
        event.put("type", RollingWindowStore.ALERT_TYPE);
        event.put("host", sample.host());
        event.put("instance", sample.instance());
        event.put("source_topic", topic);
        event.put("tte_seconds", round3(estimate.tteSeconds()));
        event.put("rate_per_second", estimate.ratePerSecond());
        event.put("used", estimate.usedCurrent());
        event.put("capacity", estimate.capacity());
        event.put("samples", estimate.samples());
        event.put("window_span_seconds", round3(estimate.windowSpanSeconds()));
        event.put("window_seconds", config.windowSeconds());
        event.put("formula", "TTE = (Capacity - Used_current) / (dUsed/dt)");
        event.put("worker", "predictive-ai-worker");

        if (inference.enabled() && config.llmOnMetricAlerts()) {
            Map<String, Object> classified = inference.classifyJson(Prompts.metricAlertMessages(
                    sample.host(),
                    sample.instance(),
                    estimate.usedCurrent(),
                    estimate.capacity(),
                    estimate.ratePerSecond(),
                    estimate.tteSeconds()));
            if (!classified.isEmpty()) {
                String sev = stringOrEmpty(classified.get("severity")).toUpperCase();
                event.put("severity", sev.isEmpty() ? null : sev);
                event.put("severity_score", asScore(classified.get("score")));
                event.put("root_cause", classified.get("root_cause"));
                event.put("summary", classified.get("summary"));
                event.put("recommended_action", classified.get("recommended_action"));
            }
        }
        return event;
    }

    private Map<String, Object> decode(String raw) {
        if (raw == null) {
            return null;
        }
        String text = raw.strip();
        if (text.isEmpty()) {
            return null;
        }
        try {
            Map<String, Object> data = objectMapper.readValue(text, new TypeReference<>() {
            });
            return data;
        } catch (Exception ex) {
            LOG.debugf("skipping non-json payload: %s", ex.getMessage());
            return null;
        }
    }

    private static Integer asScore(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static String stringOrEmpty(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static double round3(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }
}
