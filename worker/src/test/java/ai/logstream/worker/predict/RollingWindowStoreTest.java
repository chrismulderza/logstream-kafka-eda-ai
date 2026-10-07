package ai.logstream.worker.predict;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RollingWindowStoreTest {

    @Test
    void leastSquaresSlopeMatchesKnownLine() {
        // used = 10 + 2 * t  => slope 2
        double[] times = {100, 101, 102, 103};
        double[] used = {10, 12, 14, 16};
        assertEquals(2.0, RollingWindowStore.usageRate(times, used), 1e-9);
    }

    @Test
    void estimateReturnsTteWhenFilling() {
        RollingWindowStore store = new RollingWindowStore(900, 4);
        String host = "node-a";
        String instance = "/var";
        // capacity 1000, used grows by 10/s from t=0..3 => rate 10, remaining 960, TTE=96
        store.add(host, instance, 0, 10, 1000);
        store.add(host, instance, 1, 20, 1000);
        store.add(host, instance, 2, 30, 1000);
        store.add(host, instance, 3, 40, 1000);

        ExhaustionEstimate estimate = store.estimate(host, instance);
        assertNotNull(estimate);
        assertEquals(10.0, estimate.ratePerSecond(), 1e-6);
        assertEquals(40.0, estimate.usedCurrent(), 1e-9);
        assertEquals(1000.0, estimate.capacity(), 1e-9);
        assertEquals(4, estimate.samples());
        assertEquals(3.0, estimate.windowSpanSeconds(), 1e-9);
        assertEquals(96.0, estimate.tteSeconds(), 1e-6);
    }

    @Test
    void noAlertWhenRateNonPositive() {
        RollingWindowStore store = new RollingWindowStore(900, 4);
        store.add("h", "/", 0, 50, 100);
        store.add("h", "/", 1, 40, 100);
        store.add("h", "/", 2, 30, 100);
        store.add("h", "/", 3, 20, 100);
        assertNull(store.estimate("h", "/"));
    }

    @Test
    void requiresMinSamples() {
        RollingWindowStore store = new RollingWindowStore(900, 4);
        store.add("h", "/", 0, 10, 100);
        store.add("h", "/", 1, 20, 100);
        store.add("h", "/", 2, 30, 100);
        assertNull(store.estimate("h", "/"));
    }

    @Test
    void dropsPointsOutsideWindow() {
        RollingWindowStore store = new RollingWindowStore(10, 4);
        store.add("h", "/", 0, 0, 1000);
        store.add("h", "/", 1, 10, 1000);
        store.add("h", "/", 2, 20, 1000);
        store.add("h", "/", 3, 30, 1000);
        // jump ahead so early points fall outside the 10s window; need 4 fresh points
        store.add("h", "/", 20, 100, 1000);
        store.add("h", "/", 21, 110, 1000);
        store.add("h", "/", 22, 120, 1000);
        store.add("h", "/", 23, 130, 1000);

        ExhaustionEstimate estimate = store.estimate("h", "/");
        assertNotNull(estimate);
        assertEquals(4, estimate.samples());
        assertTrue(estimate.windowSpanSeconds() <= 10);
        assertEquals(10.0, estimate.ratePerSecond(), 1e-6);
    }

    @Test
    void zeroTteWhenAlreadyFull() {
        RollingWindowStore store = new RollingWindowStore(900, 4);
        store.add("h", "/", 0, 970, 1000);
        store.add("h", "/", 1, 980, 1000);
        store.add("h", "/", 2, 990, 1000);
        store.add("h", "/", 3, 1000, 1000);
        ExhaustionEstimate estimate = store.estimate("h", "/");
        assertNotNull(estimate);
        assertEquals(0.0, estimate.tteSeconds(), 1e-9);
    }
}
