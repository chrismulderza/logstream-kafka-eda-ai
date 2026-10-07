package ai.logstream.worker.predict;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per (host, instance) used/capacity samples over a rolling time window.
 * TTE = (Capacity - Used_current) / (dUsed/dt) where dUsed/dt is the
 * least-squares slope of used vs time.
 */
public class RollingWindowStore {

    public static final String ALERT_TYPE = "PREEMPTIVE_STORAGE_EXHAUSTION_RISK";

    private final double windowSeconds;
    private final int minSamples;
    private final Map<Key, Deque<Sample>> points = new ConcurrentHashMap<>();

    public RollingWindowStore(double windowSeconds, int minSamples) {
        this.windowSeconds = windowSeconds;
        this.minSamples = minSamples;
    }

    public void add(String host, String instance, double timestamp, double used, double capacity) {
        Key key = new Key(host, instance);
        Deque<Sample> series = points.computeIfAbsent(key, k -> new ArrayDeque<>());
        synchronized (series) {
            series.addLast(new Sample(timestamp, used, capacity));
            double cutoff = timestamp - windowSeconds;
            while (!series.isEmpty() && series.peekFirst().timestamp() < cutoff) {
                series.removeFirst();
            }
        }
    }

    public ExhaustionEstimate estimate(String host, String instance) {
        Deque<Sample> series = points.get(new Key(host, instance));
        if (series == null) {
            return null;
        }
        synchronized (series) {
            if (series.size() < minSamples) {
                return null;
            }
            int n = series.size();
            double[] times = new double[n];
            double[] used = new double[n];
            int i = 0;
            for (Sample sample : series) {
                times[i] = sample.timestamp();
                used[i] = sample.used();
                i++;
            }
            Sample last = series.peekLast();
            double capacity = last.capacity();
            double usedCurrent = last.used();
            double span = times[n - 1] - times[0];
            if (span <= 0) {
                return null;
            }

            double rate = usageRate(times, used);
            // Non-positive fill rate: usage is flat or shrinking — no exhaustion alert.
            if (rate <= 0) {
                return null;
            }

            double remaining = capacity - usedCurrent;
            double tte = remaining <= 0 ? 0.0 : remaining / rate;
            return new ExhaustionEstimate(tte, rate, usedCurrent, capacity, n, span);
        }
    }

    /**
     * dUsed/dt via least-squares slope on the rolling window (units per second).
     * Fits used = slope * (t - t0) + intercept.
     */
    static double usageRate(double[] times, double[] used) {
        int n = times.length;
        double t0 = times[0];
        // Normal equations for [slope, intercept] with design columns [t-t0, 1]
        double sumX = 0;
        double sumY = 0;
        double sumXX = 0;
        double sumXY = 0;
        for (int i = 0; i < n; i++) {
            double x = times[i] - t0;
            double y = used[i];
            sumX += x;
            sumY += y;
            sumXX += x * x;
            sumXY += x * y;
        }
        double denom = n * sumXX - sumX * sumX;
        if (Math.abs(denom) < 1e-18) {
            return 0.0;
        }
        return (n * sumXY - sumX * sumY) / denom;
    }

    private record Key(String host, String instance) {
        Key {
            Objects.requireNonNull(host);
            Objects.requireNonNull(instance);
        }
    }

    private record Sample(double timestamp, double used, double capacity) {
    }
}
