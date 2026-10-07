package ai.logstream.worker.parse;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Normalize PCP / raw-metrics JSON into logs and disk samples.
 */
public final class MetricsParser {

    private static final List<String> USED_NAMES = List.of(
            "filesys.used",
            "filesys.used_bytes",
            "disk.used",
            "disk.used_bytes",
            "fs.used",
            "used_bytes",
            "disk_used",
            "used"
    );
    private static final List<String> CAPACITY_NAMES = List.of(
            "filesys.capacity",
            "filesys.size",
            "disk.total",
            "disk.capacity",
            "fs.capacity",
            "capacity_bytes",
            "disk_total",
            "capacity",
            "total"
    );
    private static final List<String> PERCENT_NAMES = List.of(
            "filesys.full",
            "filesys.used_percent",
            "disk_percent",
            "used_percent"
    );

    private MetricsParser() {
    }

    public record DiskSample(String host, String instance, double used, double capacity, double timestamp) {
    }

    public record LogEvent(String host, String message, String syslogtag, String facility, String severity) {
    }

    public static double parseTimestamp(Map<String, Object> payload, double fallback) {
        for (String key : List.of("@timestamp", "timestamp", "time", "ts")) {
            if (!payload.containsKey(key)) {
                continue;
            }
            Double parsed = coerceTs(payload.get(key));
            if (parsed != null) {
                return parsed;
            }
        }
        return fallback;
    }

    public static String hostFrom(Map<String, Object> payload) {
        for (String key : List.of("host", "hostname", "@sourcehost", "source", "nodename", "node")) {
            Object value = payload.get(key);
            if (value instanceof String s && !s.isBlank()) {
                return s.strip();
            }
            if (value instanceof Map<?, ?> map) {
                Object name = map.get("name");
                if (name == null) {
                    name = map.get("hostname");
                }
                if (name instanceof String s && !s.isBlank()) {
                    return s.strip();
                }
            }
        }
        return "unknown";
    }

    public static Optional<LogEvent> extractLog(Map<String, Object> payload) {
        Object messageObj = firstNonNull(payload.get("message"), payload.get("msg"), payload.get("log"));
        if (!(messageObj instanceof String message) || message.isBlank()) {
            return Optional.empty();
        }
        if (looksLikeMetricDocument(payload) && payload.get("syslogtag") == null) {
            return Optional.empty();
        }
        return Optional.of(new LogEvent(
                hostFrom(payload),
                message.strip(),
                String.valueOf(firstNonNull(payload.get("syslogtag"), payload.get("tag"), "")),
                String.valueOf(Objects.requireNonNullElse(payload.get("facility"), "")),
                String.valueOf(firstNonNull(payload.get("severity"), payload.get("level"), ""))
        ));
    }

    public static List<DiskSample> extractDiskSamples(Map<String, Object> payload, double timestamp) {
        String host = hostFrom(payload);
        List<DiskSample> samples = new ArrayList<>();

        for (Map<String, Object> item : namedMetricItems(payload)) {
            DiskSample sample = sampleFromNamed(host, timestamp, item, payload);
            if (sample != null) {
                samples.add(sample);
            }
        }

        Map<String, Map<String, Double>> byName = metricMaps(payload);
        Map<String, Double> usedMap = firstMap(byName, USED_NAMES);
        Map<String, Double> capMap = firstMap(byName, CAPACITY_NAMES);
        Map<String, Double> pctMap = firstMap(byName, PERCENT_NAMES);

        Set<String> instances = new HashSet<>();
        instances.addAll(usedMap.keySet());
        instances.addAll(capMap.keySet());
        instances.addAll(pctMap.keySet());

        if (instances.isEmpty()) {
            double[] scalar = scalarPair(payload);
            if (scalar != null) {
                String instance = String.valueOf(
                        firstNonNull(payload.get("instance"), payload.get("mount"), "/"));
                samples.add(new DiskSample(host, instance, scalar[0], scalar[1], timestamp));
                return dedupe(samples);
            }
        }

        Iterable<String> instanceIter = instances.isEmpty() ? List.of("/") : instances;
        for (String instance : instanceIter) {
            Double used = usedMap.get(instance);
            Double capacity = capMap.get(instance);
            Double pct = pctMap.get(instance);
            if (used == null && pct != null) {
                used = pct;
                capacity = pct > 1.5 ? 100.0 : 1.0;
            }
            Double avail = byName.getOrDefault("filesys.avail", Map.of()).get(instance);
            if (used != null && capacity == null && avail != null) {
                capacity = used + avail;
            }
            if (used != null && capacity != null && capacity < used && avail != null) {
                capacity = used + avail;
            }
            if (used == null || capacity == null || capacity <= 0) {
                continue;
            }
            samples.add(new DiskSample(host, String.valueOf(instance), used, capacity, timestamp));
        }
        return dedupe(samples);
    }

    public static String utcNowIso() {
        return Instant.now().toString();
    }

    private static Double coerceTs(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            double ts = number.doubleValue();
            if (ts > 1e12) {
                return ts / 1000.0;
            }
            return ts;
        }
        if (value instanceof String text) {
            text = text.strip();
            if (text.isEmpty()) {
                return null;
            }
            try {
                return coerceTs(Double.parseDouble(text));
            } catch (NumberFormatException ignored) {
                // try ISO
            }
            try {
                if (text.endsWith("Z")) {
                    text = text.substring(0, text.length() - 1) + "+00:00";
                }
                Instant instant = OffsetDateTime.parse(text).toInstant();
                return instant.getEpochSecond() + instant.getNano() / 1_000_000_000.0;
            } catch (DateTimeParseException ignored) {
                return null;
            }
        }
        return null;
    }

    private static boolean looksLikeMetricDocument(Map<String, Object> payload) {
        Set<String> metricHints = Set.of(
                "metrics", "values", "used", "used_bytes", "capacity", "capacity_bytes", "filesys", "disk");
        for (String key : payload.keySet()) {
            if (metricHints.contains(key)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> namedMetricItems(Map<String, Object> payload) {
        List<Map<String, Object>> items = new ArrayList<>();
        for (String key : List.of("values", "metrics", "data")) {
            Object block = payload.get(key);
            if (block instanceof List<?> list) {
                for (Object item : list) {
                    if (item instanceof Map<?, ?> map) {
                        items.add((Map<String, Object>) map);
                    }
                }
            }
        }
        return items;
    }

    private static DiskSample sampleFromNamed(
            String host, double timestamp, Map<String, Object> item, Map<String, Object> parent) {
        String name = String.valueOf(firstNonNull(item.get("name"), item.get("metric"), ""));
        String instance = String.valueOf(
                firstNonNull(item.get("instance"), item.get("inst"), parent.get("mount"), "/"));
        Double value = toFloat(item.containsKey("value") ? item.get("value") : item.get("val"));
        if (value == null) {
            return null;
        }
        Double used = null;
        Double capacity = null;
        String lname = name.toLowerCase();
        if (lname.contains("used")) {
            used = value;
        }
        if (lname.contains("capacity") || lname.contains("size") || lname.contains("total")) {
            capacity = value;
        }
        if (used != null && capacity == null) {
            capacity = toFloat(firstNonNull(item.get("capacity"), item.get("total")));
        }
        if (used == null || capacity == null || capacity <= 0) {
            return null;
        }
        return new DiskSample(host, instance, used, capacity, timestamp);
    }

    private static Map<String, Map<String, Double>> metricMaps(Map<String, Object> payload) {
        Map<String, Map<String, Double>> found = new HashMap<>();
        walkMetrics(payload, found, "");
        for (Map<String, Object> item : namedMetricItems(payload)) {
            String name = String.valueOf(firstNonNull(item.get("name"), item.get("metric"), ""));
            if (name.isBlank() || "null".equals(name)) {
                continue;
            }
            String instance = String.valueOf(firstNonNull(item.get("instance"), item.get("inst"), "/"));
            Double value = toFloat(item.containsKey("value") ? item.get("value") : item.get("val"));
            if (value == null) {
                continue;
            }
            found.computeIfAbsent(name, k -> new HashMap<>()).put(instance, value);
        }
        return found;
    }

    private static void walkMetrics(Object node, Map<String, Map<String, Double>> found, String path) {
        if (node instanceof Map<?, ?> map) {
            Map<String, Double> numericChildren = new HashMap<>();
            boolean allNumeric = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Double num = toFloat(entry.getValue());
                if (num != null) {
                    numericChildren.put(String.valueOf(entry.getKey()), num);
                } else {
                    allNumeric = false;
                }
            }
            if (!path.isEmpty() && allNumeric && !numericChildren.isEmpty() && numericChildren.size() == map.size()) {
                found.computeIfAbsent(path, k -> new HashMap<>()).putAll(numericChildren);
                return;
            }
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = String.valueOf(entry.getKey());
                if (key.startsWith("@")) {
                    continue;
                }
                String childPath = path.isEmpty() ? key : path + "." + key;
                Double scalar = toFloat(entry.getValue());
                if (scalar != null && (path.equals("filesys") || path.equals("disk") || path.equals("fs") || path.isEmpty())) {
                    found.computeIfAbsent(childPath, k -> new HashMap<>()).putIfAbsent("/", scalar);
                }
                walkMetrics(entry.getValue(), found, childPath);
            }
        } else if (node instanceof List<?> list) {
            for (Object item : list) {
                walkMetrics(item, found, path);
            }
        }
    }

    private static Map<String, Double> firstMap(Map<String, Map<String, Double>> byName, List<String> names) {
        for (String name : names) {
            Map<String, Double> direct = byName.get(name);
            if (direct != null && !direct.isEmpty()) {
                return direct;
            }
            for (Map.Entry<String, Map<String, Double>> entry : byName.entrySet()) {
                if (entry.getKey().equals(name) || entry.getKey().endsWith(name)) {
                    return entry.getValue();
                }
            }
        }
        return Map.of();
    }

    private static double[] scalarPair(Map<String, Object> payload) {
        Double used = null;
        Double capacity = null;
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            String lkey = entry.getKey().toLowerCase();
            Double number = toFloat(entry.getValue());
            if (number == null) {
                continue;
            }
            if (Set.of("used", "used_bytes", "disk_used", "filesys.used").contains(lkey)) {
                used = number;
            }
            if (Set.of("capacity", "capacity_bytes", "total", "disk_total", "filesys.capacity").contains(lkey)) {
                capacity = number;
            }
        }
        if (used == null || capacity == null || capacity <= 0) {
            return null;
        }
        return new double[]{used, capacity};
    }

    private static Double toFloat(Object value) {
        if (value == null || value instanceof Boolean) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String s) {
            try {
                return Double.parseDouble(s.strip());
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }

    private static List<DiskSample> dedupe(List<DiskSample> samples) {
        Map<String, DiskSample> seen = new LinkedHashMap<>();
        for (DiskSample sample : samples) {
            seen.put(sample.host() + "\0" + sample.instance(), sample);
        }
        return new ArrayList<>(seen.values());
    }

    private static Object firstNonNull(Object... values) {
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
