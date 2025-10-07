package ru.miacomsoft.mumpsdb.embedding;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EmbeddingStorage {
    private final Map<String, List<EmbeddingRecord>> globalEmbeddings = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final EmbeddingService embeddingService;

    public EmbeddingStorage(EmbeddingService embeddingService) {
        this.embeddingService = embeddingService;
    }

    public void storeEmbedding(String global, Object[] path, Object value, float[] embedding) {
        if (!embeddingService.isEmbeddingEnabled()) {
            return; // Пропускаем сохранение embedding если функция отключена
        }

        lock.writeLock().lock();
        try {
            EmbeddingRecord record = new EmbeddingRecord(global, path, value, embedding);
            globalEmbeddings.computeIfAbsent(global, k -> new ArrayList<>()).add(record);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeEmbedding(String global, Object[] path) {
        if (!embeddingService.isEmbeddingEnabled()) {
            return; // Пропускаем удаление embedding если функция отключена
        }

        lock.writeLock().lock();
        try {
            List<EmbeddingRecord> records = globalEmbeddings.get(global);
            if (records != null) {
                records.removeIf(record -> Arrays.equals(record.getPath(), path));
                if (records.isEmpty()) {
                    globalEmbeddings.remove(global);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeAllEmbeddings(String global) {
        if (!embeddingService.isEmbeddingEnabled()) {
            return; // Пропускаем удаление embedding если функция отключена
        }

        lock.writeLock().lock();
        try {
            globalEmbeddings.remove(global);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<VectorSearchResult> similaritySearch(String query, int topK) throws Exception {
        if (!embeddingService.isEmbeddingEnabled()) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }

        return similaritySearch(query, topK, null);
    }

    public List<VectorSearchResult> similaritySearch(String query, int topK, String targetGlobal) throws Exception {
        if (!embeddingService.isEmbeddingEnabled()) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }

        float[] queryEmbedding = embeddingService.getEmbedding(query);

        lock.readLock().lock();
        try {
            List<VectorSearchResult> results = new ArrayList<>();

            // Поиск по всем глобалам или только по указанному
            Collection<String> globalsToSearch = (targetGlobal != null)
                    ? Collections.singletonList(targetGlobal)
                    : globalEmbeddings.keySet();

            for (String global : globalsToSearch) {
                List<EmbeddingRecord> records = globalEmbeddings.get(global);
                if (records != null) {
                    for (EmbeddingRecord record : records) {
                        double similarity = embeddingService.calculateSimilarity(
                                queryEmbedding, record.getEmbedding()
                        );
                        results.add(new VectorSearchResult(
                                record.getGlobal(),
                                record.getPath(),
                                record.getValue(),
                                record.getEmbedding(),
                                similarity
                        ));
                    }
                }
            }

            // Сортировка по сходству и возврат topK результатов
            results.sort((a, b) -> Double.compare(b.getSimilarity(), a.getSimilarity()));
            return results.subList(0, Math.min(topK, results.size()));
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<VectorSearchResult> exactSearch(String query, String targetGlobal) {
        if (!embeddingService.isEmbeddingEnabled()) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }

        lock.readLock().lock();
        try {
            List<VectorSearchResult> results = new ArrayList<>();

            Collection<String> globalsToSearch = (targetGlobal != null)
                    ? Collections.singletonList(targetGlobal)
                    : globalEmbeddings.keySet();

            for (String global : globalsToSearch) {
                List<EmbeddingRecord> records = globalEmbeddings.get(global);
                if (records != null) {
                    for (EmbeddingRecord record : records) {
                        String valueStr = record.getValue().toString().toLowerCase();
                        if (valueStr.contains(query.toLowerCase())) {
                            results.add(new VectorSearchResult(
                                    record.getGlobal(),
                                    record.getPath(),
                                    record.getValue(),
                                    record.getEmbedding(),
                                    1.0 // Полное совпадение для точного поиска
                            ));
                        }
                    }
                }
            }
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getEmbeddingCount() {
        if (!embeddingService.isEmbeddingEnabled()) {
            return 0; // Возвращаем 0 если функция отключена
        }

        lock.readLock().lock();
        try {
            return globalEmbeddings.values().stream().mapToInt(List::size).sum();
        } finally {
            lock.readLock().unlock();
        }
    }

    // Внутренний класс для хранения записей с embedding
    private static class EmbeddingRecord {
        private final String global;
        private final Object[] path;
        private final Object value;
        private final float[] embedding;

        public EmbeddingRecord(String global, Object[] path, Object value, float[] embedding) {
            this.global = global;
            this.path = path;
            this.value = value;
            this.embedding = embedding;
        }

        public String getGlobal() { return global; }
        public Object[] getPath() { return path; }
        public Object getValue() { return value; }
        public float[] getEmbedding() { return embedding; }
    }
}