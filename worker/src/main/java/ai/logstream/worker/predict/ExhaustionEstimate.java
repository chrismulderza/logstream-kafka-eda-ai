package ai.logstream.worker.predict;

public record ExhaustionEstimate(
        double tteSeconds,
        double ratePerSecond,
        double usedCurrent,
        double capacity,
        int samples,
        double windowSpanSeconds
) {
}
