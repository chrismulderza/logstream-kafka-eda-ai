package ai.logstream.worker.infer;

import java.util.List;

public final class Prompts {

    static final String RCA_SYSTEM = """
            You are a platform SRE assistant. Analyze one RHEL syslog line.
            Respond with compact JSON only:
            {"root_cause":"...","subsystem":"...","summary":"...","recommended_action":"..."}
            Do not invent host facts that are not in the message.""";

    static final String SEVERITY_SYSTEM = """
            Score operational severity of one event.
            Respond with compact JSON only:
            {"severity":"INFO|WARNING|CRITICAL","score":<0-100 integer>,"rationale":"..."}""";

    static final String METRIC_ALERT_SYSTEM = """
            Explain a predictive storage exhaustion alert.
            Respond with compact JSON only:
            {"severity":"WARNING|CRITICAL","score":<0-100 integer>,"root_cause":"...","summary":"...","recommended_action":"..."}""";

    private Prompts() {
    }

    public static List<ChatMessage> rcaMessages(
            String host, String syslogtag, String facility, String severity, String message) {
        String user = "host=" + host + "\nsyslogtag=" + syslogtag + "\nfacility=" + facility
                + "\nseverity=" + severity + "\nmessage=" + message + "\n";
        return List.of(
                new ChatMessage("system", RCA_SYSTEM),
                new ChatMessage("user", user)
        );
    }

    public static List<ChatMessage> severityMessages(String host, String eventContext, String message) {
        String user = "host=" + host + "\ncontext=" + eventContext + "\nmessage=" + message + "\n";
        return List.of(
                new ChatMessage("system", SEVERITY_SYSTEM),
                new ChatMessage("user", user)
        );
    }

    public static List<ChatMessage> metricAlertMessages(
            String host,
            String instance,
            double used,
            double capacity,
            double ratePerSecond,
            double tteSeconds) {
        String user = "host=" + host + " instance=" + instance + " used=" + used + " capacity=" + capacity
                + " rate_per_second=" + ratePerSecond + " tte_seconds=" + tteSeconds + "\n";
        return List.of(
                new ChatMessage("system", METRIC_ALERT_SYSTEM),
                new ChatMessage("user", user)
        );
    }
}
