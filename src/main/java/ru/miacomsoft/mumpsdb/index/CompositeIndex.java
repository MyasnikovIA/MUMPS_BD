
package ru.miacomsoft.mumpsdb.index;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.index.IndexResult;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Составные индексы для сложных запросов
 */
public class CompositeIndex {
    private final Map<String, CompositeIndexEntry> indexes = new ConcurrentHashMap<>();
    private final Database database;

    public CompositeIndex(Database database) {
        this.database = database;
    }

    /**
     * Создание составного индекса
     */
    public void createIndex(String indexName, String[] fields, IndexType type) {
        CompositeIndexEntry index = new CompositeIndexEntry(indexName, fields, type);
        indexes.put(indexName, index);

        System.out.println("Created composite index: " + indexName + " for fields: " + Arrays.toString(fields));
    }

    /**
     * Индексация нескольких полей
     */
    public void indexMultipleFields(String global, Object[] fields, Object value) {
        for (CompositeIndexEntry index : indexes.values()) {
            if (index.matchesFields(fields)) {
                index.addEntry(global, fields, value);
            }
        }
    }

    /**
     * Поиск по составному индексу
     */
    public List<IndexResult> search(String indexName, Object[] query) {
        CompositeIndexEntry index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException("Index not found: " + indexName);
        }

        return index.search(query);
    }

    /**
     * Поиск по диапазону
     */
    public List<IndexResult> rangeSearch(String indexName, Object[] minValues, Object[] maxValues) {
        CompositeIndexEntry index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException("Index not found: " + indexName);
        }

        return index.rangeSearch(minValues, maxValues);
    }

    /**
     * Удаление индекса
     */
    public void removeIndex(String indexName) {
        indexes.remove(indexName);
        System.out.println("Removed index: " + indexName);
    }

    /**
     * Получение статистики индекса
     */
    public Map<String, Object> getIndexStats(String indexName) {
        CompositeIndexEntry index = indexes.get(indexName);
        if (index == null) {
            return Collections.emptyMap();
        }

        return index.getStats();
    }

    /**
     * Получение всех индексов
     */
    public Set<String> getIndexNames() {
        return indexes.keySet();
    }

    /**
     * Оптимизация индексов
     */
    public void optimizeIndexes() {
        for (CompositeIndexEntry index : indexes.values()) {
            index.optimize();
        }
        System.out.println("Index optimization completed");
    }

    /**
     * Тип индекса
     */
    public enum IndexType {
        HASH,       // Для точных совпадений
        RANGE,      // Для диапазонных запросов
        SPATIAL,    // Для пространственных данных
        FULLTEXT    // Для полнотекстового поиска
    }
}

/**
 * Запись составного индекса
 */
class CompositeIndexEntry {
    private final String name;
    private final String[] fields;
    private final CompositeIndex.IndexType type;
    private final Map<IndexKey, Set<IndexValue>> indexData = new ConcurrentHashMap<>();
    private final TreeMap<IndexKey, Set<IndexValue>> rangeIndex = new TreeMap<>();
    private long entryCount;
    private long lastOptimized;

    public CompositeIndexEntry(String name, String[] fields, CompositeIndex.IndexType type) {
        this.name = name;
        this.fields = fields.clone();
        this.type = type;
        this.entryCount = 0;
        this.lastOptimized = System.currentTimeMillis();
    }

    /**
     * Проверка соответствия полей
     */
    public boolean matchesFields(Object[] fieldsToCheck) {
        if (fieldsToCheck.length != fields.length) {
            return false;
        }

        for (int i = 0; i < fields.length; i++) {
            // Проверяем, что поля совпадают по типу и назначению
            if (!isCompatibleField(fields[i], fieldsToCheck[i])) {
                return false;
            }
        }

        return true;
    }

    private boolean isCompatibleField(String expected, Object actual) {
        // Здесь может быть более сложная логика проверки типов
        return true;
    }

    /**
     * Добавление записи в индекс
     */
    public void addEntry(String global, Object[] fieldValues, Object value) {
        IndexKey key = new IndexKey(fieldValues);
        IndexValue indexValue = new IndexValue(global, fieldValues, value, System.currentTimeMillis());

        indexData.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(indexValue);

        if (type == CompositeIndex.IndexType.RANGE) {
            rangeIndex.put(key, indexData.get(key));
        }

        entryCount++;

        // Авто-оптимизация при достижении лимита
        if (entryCount % 1000 == 0) {
            optimize();
        }
    }

    /**
     * Поиск по индексу
     */
    public List<IndexResult> search(Object[] query) {
        IndexKey searchKey = new IndexKey(query);
        Set<IndexValue> results = indexData.get(searchKey);

        if (results == null) {
            return Collections.emptyList();
        }

        return results.stream()
                .map(value -> new IndexResult(
                        value.getGlobal(),
                        value.getPath(),
                        value.getValue(),
                        1.0,
                        name
                ))
                .collect(Collectors.toList());
    }

    /**
     * Поиск по диапазону
     */
    public List<IndexResult> rangeSearch(Object[] minValues, Object[] maxValues) {
        if (type != CompositeIndex.IndexType.RANGE) {
            throw new IllegalStateException("Range search only available for RANGE indexes");
        }

        IndexKey minKey = new IndexKey(minValues);
        IndexKey maxKey = new IndexKey(maxValues);

        List<IndexResult> results = new ArrayList<>();

        for (Map.Entry<IndexKey, Set<IndexValue>> entry :
                rangeIndex.subMap(minKey, true, maxKey, true).entrySet()) {

            for (IndexValue indexValue : entry.getValue()) {
                IndexResult result = new IndexResult(
                        indexValue.getGlobal(),
                        indexValue.getPath(),
                        indexValue.getValue(),
                        1.0,
                        name
                );
                results.add(result);
            }
        }

        return results;
    }

    /**
     * Оптимизация индекса
     */
    public void optimize() {
        // Удаление устаревших записей
        long cutoffTime = System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000); // 7 дней
        int removed = 0;

        for (Iterator<Map.Entry<IndexKey, Set<IndexValue>>> it = indexData.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<IndexKey, Set<IndexValue>> entry = it.next();
            entry.getValue().removeIf(value -> value.getTimestamp() < cutoffTime);

            if (entry.getValue().isEmpty()) {
                it.remove();
                removed++;
            }
        }

        lastOptimized = System.currentTimeMillis();
        System.out.println("Optimized index " + name + ", removed " + removed + " stale entries");
    }

    /**
     * Получение статистики
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("name", name);
        stats.put("type", type);
        stats.put("fields", Arrays.toString(fields));
        stats.put("entryCount", entryCount);
        stats.put("uniqueKeys", indexData.size());
        stats.put("lastOptimized", new Date(lastOptimized));
        stats.put("memoryUsage", estimateMemoryUsage());
        return stats;
    }

    private long estimateMemoryUsage() {
        return entryCount * 100L; // Примерная оценка
    }

    // Getters
    public String getName() {
        return name;
    }

    public String[] getFields() {
        return fields.clone();
    }

    public CompositeIndex.IndexType getType() {
        return type;
    }
}

/**
 * Ключ индекса
 */
class IndexKey implements Comparable<IndexKey> {
    private final Object[] values;
    private final int hashCode;

    public IndexKey(Object[] values) {
        this.values = values.clone();
        this.hashCode = Arrays.deepHashCode(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexKey indexKey = (IndexKey) o;
        return Arrays.deepEquals(values, indexKey.values);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public int compareTo(IndexKey other) {
        for (int i = 0; i < Math.min(values.length, other.values.length); i++) {
            @SuppressWarnings("unchecked")
            Comparable<Object> thisVal = (Comparable<Object>) values[i];
            @SuppressWarnings("unchecked")
            Comparable<Object> otherVal = (Comparable<Object>) other.values[i];

            int comparison = thisVal.compareTo(otherVal);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(values.length, other.values.length);
    }

    @Override
    public String toString() {
        return "IndexKey" + Arrays.toString(values);
    }
}

/**
 * Значение индекса
 */
class IndexValue {
    private final String global;
    private final Object[] path;
    private final Object value;
    private final long timestamp;

    public IndexValue(String global, Object[] path, Object value, long timestamp) {
        this.global = global;
        this.path = path != null ? path.clone() : new Object[0];
        this.value = value;
        this.timestamp = timestamp;
    }

    // Getters
    public String getGlobal() {
        return global;
    }

    public Object[] getPath() {
        return path != null ? path.clone() : new Object[0];
    }

    public Object getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexValue that = (IndexValue) o;
        return Objects.equals(global, that.global) &&
                Arrays.deepEquals(path, that.path) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(global, Arrays.deepHashCode(path), value);
    }

    @Override
    public String toString() {
        return String.format("IndexValue{global='%s', path=%s, value=%s}",
                global, Arrays.toString(path), value);
    }
}