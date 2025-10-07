package ru.miacomsoft.mumpsdb.benchmark;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.monitoring.MetricsCollector;

import java.util.*;
import java.util.concurrent.*;

public class DatabaseBenchmark {
    private final Database database;
    private final MetricsCollector metrics;
    private final ExecutorService executor;
    private final Random random = new Random();

    public DatabaseBenchmark(Database database) {
        this.database = database;
        this.metrics = MetricsCollector.getInstance();
        this.executor = Executors.newCachedThreadPool();
    }

    public BenchmarkResult runComprehensiveBenchmark() {
        BenchmarkResult result = new BenchmarkResult();

        System.out.println("Starting comprehensive database benchmark...");

        // 1. Тест записи
        result.setWriteTest(runWriteBenchmark());

        // 2. Тест чтения
        result.setReadTest(runReadBenchmark());

        // 3. Тест конкурентного доступа
        result.setConcurrencyTest(runConcurrencyBenchmark());

        // 4. Тест поиска
        result.setSearchTest(runSearchBenchmark());

        System.out.println("Benchmark completed!");
        return result;
    }

    private BenchmarkResult.TestResult runWriteBenchmark() {
        long startTime = System.currentTimeMillis();
        int operations = 10000;

        for (int i = 0; i < operations; i++) {
            String global = "^Benchmark" + (i % 100);
            Object[] path = {"data", i};
            String value = "value_" + i + "_" + random.nextInt(1000);

            database.set(global, value, path);
        }

        long duration = System.currentTimeMillis() - startTime;
        double opsPerSecond = (double) operations / (duration / 1000.0);

        return new BenchmarkResult.TestResult("Write", operations, duration, opsPerSecond);
    }

    private BenchmarkResult.TestResult runReadBenchmark() {
        long startTime = System.currentTimeMillis();
        int operations = 10000;
        int found = 0;

        for (int i = 0; i < operations; i++) {
            String global = "^Benchmark" + (i % 100);
            Object[] path = {"data", i};

            Object value = database.get(global, path);
            if (value != null) {
                found++;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        double opsPerSecond = (double) operations / (duration / 1000.0);

        BenchmarkResult.TestResult result = new BenchmarkResult.TestResult("Read", operations, duration, opsPerSecond);
        result.setAdditionalMetric("hit_rate", (double) found / operations);
        return result;
    }

    private BenchmarkResult.TestResult runConcurrencyBenchmark() {
        int threadCount = 10;
        int operationsPerThread = 1000;
        List<Future<Long>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Future<Long> future = executor.submit(() -> {
                long threadStart = System.currentTimeMillis();

                for (int i = 0; i < operationsPerThread; i++) {
                    String global = "^Concurrent" + threadId;
                    Object[] path = {"thread", threadId, "data", i};
                    String value = "concurrent_value_" + i;

                    if (i % 2 == 0) {
                        database.set(global, value, path);
                    } else {
                        database.get(global, path);
                    }
                }

                return System.currentTimeMillis() - threadStart;
            });
            futures.add(future);
        }

        // Ждем завершения всех потоков
        long totalThreadTime = 0;
        for (Future<Long> future : futures) {
            try {
                totalThreadTime += future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        long totalDuration = System.currentTimeMillis() - startTime;
        int totalOperations = threadCount * operationsPerThread;
        double opsPerSecond = (double) totalOperations / (totalDuration / 1000.0);

        BenchmarkResult.TestResult result = new BenchmarkResult.TestResult("Concurrency", totalOperations, totalDuration, opsPerSecond);
        result.setAdditionalMetric("threads", threadCount);
        result.setAdditionalMetric("avg_thread_time", (double) totalThreadTime / threadCount);

        return result;
    }

    private BenchmarkResult.TestResult runSearchBenchmark() {
        // Сначала создаем данные для поиска
        for (int i = 0; i < 1000; i++) {
            database.set("^SearchTest", "search_value_" + i, "data", i);
        }

        long startTime = System.currentTimeMillis();
        int operations = 1000;
        int found = 0;

        for (int i = 0; i < operations; i++) {
            String searchValue = "search_value_" + random.nextInt(1000);
            List<Database.SearchResult> results = database.fastSearch(searchValue);
            if (!results.isEmpty()) {
                found++;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        double opsPerSecond = (double) operations / (duration / 1000.0);

        BenchmarkResult.TestResult result = new BenchmarkResult.TestResult("Search", operations, duration, opsPerSecond);
        result.setAdditionalMetric("success_rate", (double) found / operations);

        return result;
    }

    public void shutdown() {
        executor.shutdown();
    }

    public static class BenchmarkResult {
        private TestResult writeTest;
        private TestResult readTest;
        private TestResult concurrencyTest;
        private TestResult searchTest;

        // Getters and setters
        public TestResult getWriteTest() { return writeTest; }
        public void setWriteTest(TestResult writeTest) { this.writeTest = writeTest; }
        public TestResult getReadTest() { return readTest; }
        public void setReadTest(TestResult readTest) { this.readTest = readTest; }
        public TestResult getConcurrencyTest() { return concurrencyTest; }
        public void setConcurrencyTest(TestResult concurrencyTest) { this.concurrencyTest = concurrencyTest; }
        public TestResult getSearchTest() { return searchTest; }
        public void setSearchTest(TestResult searchTest) { this.searchTest = searchTest; }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== DATABASE BENCHMARK RESULTS ===\n");
            sb.append(formatTestResult(writeTest));
            sb.append(formatTestResult(readTest));
            sb.append(formatTestResult(concurrencyTest));
            sb.append(formatTestResult(searchTest));
            return sb.toString();
        }

        private String formatTestResult(TestResult result) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n").append(result.name).append(" Test:\n");
            sb.append(String.format("  Operations: %,d\n", result.operations));
            sb.append(String.format("  Duration: %,d ms\n", result.duration));
            sb.append(String.format("  Throughput: %,.2f ops/sec\n", result.operationsPerSecond));

            if (result.additionalMetrics != null) {
                result.additionalMetrics.forEach((key, value) ->
                        sb.append(String.format("  %s: %s\n", key, value)));
            }

            return sb.toString();
        }

        public static class TestResult {
            private final String name;
            private final int operations;
            private final long duration;
            private final double operationsPerSecond;
            private Map<String, Object> additionalMetrics;

            public TestResult(String name, int operations, long duration, double operationsPerSecond) {
                this.name = name;
                this.operations = operations;
                this.duration = duration;
                this.operationsPerSecond = operationsPerSecond;
            }

            public void setAdditionalMetric(String key, Object value) {
                if (additionalMetrics == null) {
                    additionalMetrics = new HashMap<>();
                }
                additionalMetrics.put(key, value);
            }

            // Getters
            public String getName() { return name; }
            public int getOperations() { return operations; }
            public long getDuration() { return duration; }
            public double getOperationsPerSecond() { return operationsPerSecond; }
            public Map<String, Object> getAdditionalMetrics() { return additionalMetrics; }
        }
    }
}