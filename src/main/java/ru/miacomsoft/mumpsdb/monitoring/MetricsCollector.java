package ru.miacomsoft.mumpsdb.monitoring;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.Map;

public class MetricsCollector {
    private static MetricsCollector instance;

    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();

    private MetricsCollector() {}

    public static synchronized MetricsCollector getInstance() {
        if (instance == null) {
            instance = new MetricsCollector();
        }
        return instance;
    }

    public void incrementCounter(String name) {
        counters.computeIfAbsent(name, k -> new LongAdder()).increment();
    }

    public void incrementCounter(String name, long value) {
        counters.computeIfAbsent(name, k -> new LongAdder()).add(value);
    }

    public void setGauge(String name, long value) {
        gauges.computeIfAbsent(name, k -> new AtomicLong()).set(value);
    }

    public void recordHistogram(String name, long value) {
        histograms.computeIfAbsent(name, k -> new Histogram()).record(value);
    }

    public void recordOperationTime(String operation, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        recordHistogram(operation + "_time", duration);
        incrementCounter(operation + "_calls");
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new ConcurrentHashMap<>();

        // Counters
        counters.forEach((name, counter) ->
                metrics.put("counter_" + name, counter.longValue()));

        // Gauges
        gauges.forEach((name, gauge) ->
                metrics.put("gauge_" + name, gauge.get()));

        // Histograms
        histograms.forEach((name, histogram) ->
                metrics.put("histogram_" + name, histogram.getStats()));

        return metrics;
    }

    public void reset() {
        counters.clear();
        gauges.clear();
        histograms.clear();
    }

    private static class Histogram {
        private final LongAdder count = new LongAdder();
        private final LongAdder sum = new LongAdder();
        private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

        public void record(long value) {
            count.increment();
            sum.add(value);
            min.updateAndGet(current -> Math.min(current, value));
            max.updateAndGet(current -> Math.max(current, value));
        }

        public Map<String, Object> getStats() {
            Map<String, Object> stats = new ConcurrentHashMap<>();
            stats.put("count", count.longValue());
            stats.put("sum", sum.longValue());
            stats.put("min", count.longValue() > 0 ? min.get() : 0);
            stats.put("max", count.longValue() > 0 ? max.get() : 0);
            stats.put("avg", count.longValue() > 0 ? sum.longValue() / count.longValue() : 0);
            return stats;
        }
    }
}