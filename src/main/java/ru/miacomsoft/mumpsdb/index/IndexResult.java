
package ru.miacomsoft.mumpsdb.index;

import java.util.Arrays;
import java.util.Objects;
import java.util.Date;

/**
 * Результат поиска по составному индексу
 * Содержит информацию о найденном значении, его глобале, пути и релевантности
 */
public class IndexResult {
    private final String global;
    private final Object[] path;
    private final Object value;
    private final double score;
    private final String indexName;
    private final long timestamp;

    /**
     * Конструктор результата поиска по индексу
     *
     * @param global имя глобала
     * @param path   путь к значению
     * @param value  найденное значение
     * @param score  оценка релевантности (0.0 - 1.0)
     */
    public IndexResult(String global, Object[] path, Object value, double score) {
        this.global = global;
        this.path = path != null ? path.clone() : new Object[0];
        this.value = value;
        this.score = Math.max(0.0, Math.min(1.0, score)); // Нормализуем score в диапазон [0, 1]
        this.indexName = null;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Конструктор с указанием имени индекса
     *
     * @param global    имя глобала
     * @param path      путь к значению
     * @param value     найденное значение
     * @param score     оценка релевантности
     * @param indexName имя индекса, использованного для поиска
     */
    public IndexResult(String global, Object[] path, Object value, double score, String indexName) {
        this.global = global;
        this.path = path != null ? path.clone() : new Object[0];
        this.value = value;
        this.score = Math.max(0.0, Math.min(1.0, score));
        this.indexName = indexName;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Конструктор для точного совпадения (score = 1.0)
     *
     * @param global имя глобала
     * @param path   путь к значению
     * @param value  найденное значение
     */
    public IndexResult(String global, Object[] path, Object value) {
        this(global, path, value, 1.0, null);
    }

    // Getters

    /**
     * @return имя глобала, в котором найдено значение
     */
    public String getGlobal() {
        return global;
    }

    /**
     * @return путь к значению в глобале
     */
    public Object[] getPath() {
        return path != null ? path.clone() : new Object[0];
    }

    /**
     * @return найденное значение
     */
    public Object getValue() {
        return value;
    }

    /**
     * @return оценка релевантности (0.0 - 1.0), где 1.0 - полное совпадение
     */
    public double getScore() {
        return score;
    }

    /**
     * @return имя индекса, использованного для поиска
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * @return временная метка создания результата
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Проверяет, является ли результат точным совпадением (score = 1.0)
     *
     * @return true если это точное совпадение
     */
    public boolean isExactMatch() {
        return Math.abs(score - 1.0) < 0.0001;
    }

    /**
     * Проверяет, является ли результат релевантным (score выше порога)
     *
     * @param threshold порог релевантности (по умолчанию 0.7)
     * @return true если результат считается релевантным
     */
    public boolean isRelevant(double threshold) {
        return score >= threshold;
    }

    public boolean isRelevant() {
        return isRelevant(0.7);
    }

    /**
     * Форматирует путь в строковое представление
     *
     * @return отформатированный путь
     */
    public String getFormattedPath() {
        if (path.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < path.length; i++) {
            if (i > 0) sb.append(",");
            Object element = path[i];
            if (element instanceof String) {
                sb.append("\"").append(element).append("\"");
            } else {
                sb.append(element);
            }
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Форматирует значение в строковое представление
     *
     * @return отформатированное значение
     */
    public String getFormattedValue() {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "\"" + value + "\"";
        } else {
            return value.toString();
        }
    }

    /**
     * Создает копию результата с новым score
     *
     * @param newScore новый показатель релевантности
     * @return новый объект IndexResult
     */
    public IndexResult withScore(double newScore) {
        return new IndexResult(global, path, value, newScore, indexName);
    }

    /**
     * Создает копию результата с указанием имени индекса
     *
     * @param newIndexName имя индекса
     * @return новый объект IndexResult
     */
    public IndexResult withIndexName(String newIndexName) {
        return new IndexResult(global, path, value, score, newIndexName);
    }

    @Override
    public String toString() {
        return String.format(
                "IndexResult{global='%s', path=%s, value=%s, score=%.4f, index='%s'}",
                global,
                Arrays.toString(path),
                getFormattedValue(),
                score,
                indexName != null ? indexName : "unknown"
        );
    }

    /**
     * Детальное строковое представление для отладки
     *
     * @return детальная информация о результате
     */
    public String toDetailedString() {
        return String.format(
                "=== INDEX SEARCH RESULT ===\n" +
                        "Global: %s\n" +
                        "Path: %s\n" +
                        "Value: %s\n" +
                        "Score: %.4f\n" +
                        "Index: %s\n" +
                        "Exact Match: %s\n" +
                        "Relevant: %s\n" +
                        "Timestamp: %s",
                global,
                getFormattedPath(),
                getFormattedValue(),
                score,
                indexName != null ? indexName : "N/A",
                isExactMatch() ? "YES" : "NO",
                isRelevant() ? "YES" : "NO",
                new Date(timestamp)
        );
    }

    /**
     * Краткое строковое представление для пользовательского вывода
     *
     * @return краткая информация о результате
     */
    public String toShortString() {
        return String.format(
                "%s%s = %s (score: %.2f)",
                global,
                getFormattedPath(),
                getFormattedValue(),
                score
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexResult that = (IndexResult) o;

        if (Double.compare(that.score, score) != 0) return false;
        if (!Objects.equals(global, that.global)) return false;
        if (!Arrays.deepEquals(path, that.path)) return false;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(global, value, score);
        result = 31 * result + Arrays.deepHashCode(path);
        return result;
    }

    /**
     * Сравнивает результаты по релевантности (для сортировки)
     *
     * @param other другой результат для сравнения
     * @return отрицательное число если этот результат более релевантен,
     * положительное если менее, 0 если равны
     */
    public int compareByScore(IndexResult other) {
        return Double.compare(other.score, this.score); // Сортировка по убыванию score
    }

    /**
     * Сравнивает результаты по имени глобала и пути
     *
     * @param other другой результат для сравнения
     * @return результат сравнения
     */
    public int compareByGlobalAndPath(IndexResult other) {
        int globalCompare = this.global.compareTo(other.global);
        if (globalCompare != 0) return globalCompare;

        // Сравниваем пути лексикографически
        int minLength = Math.min(this.path.length, other.path.length);
        for (int i = 0; i < minLength; i++) {
            @SuppressWarnings("unchecked")
            Comparable<Object> thisElement = (Comparable<Object>) this.path[i];
            @SuppressWarnings("unchecked")
            Comparable<Object> otherElement = (Comparable<Object>) other.path[i];

            int elementCompare = thisElement.compareTo(otherElement);
            if (elementCompare != 0) return elementCompare;
        }

        return Integer.compare(this.path.length, other.path.length);
    }

    /**
     * Проверяет, ссылается ли результат на тот же узел данных
     *
     * @param other другой результат для проверки
     * @return true если результаты ссылаются на один узел
     */
    public boolean referencesSameNode(IndexResult other) {
        return Objects.equals(global, other.global) &&
                Arrays.deepEquals(path, other.path);
    }

    /**
     * Создает результат с нормализованным путем (приводит типы к строковым)
     *
     * @return новый результат с нормализованным путем
     */
    public IndexResult withNormalizedPath() {
        Object[] normalizedPath = new Object[path.length];
        for (int i = 0; i < path.length; i++) {
            normalizedPath[i] = path[i] != null ? path[i].toString() : "null";
        }
        return new IndexResult(global, normalizedPath, value, score, indexName);
    }

    /**
     * Билдер для создания IndexResult
     */
    public static class Builder {
        private String global;
        private Object[] path;
        private Object value;
        private double score = 1.0;
        private String indexName;

        public Builder setGlobal(String global) {
            this.global = global;
            return this;
        }

        public Builder setPath(Object[] path) {
            this.path = path != null ? path.clone() : new Object[0];
            return this;
        }

        public Builder setValue(Object value) {
            this.value = value;
            return this;
        }

        public Builder setScore(double score) {
            this.score = score;
            return this;
        }

        public Builder setIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public IndexResult build() {
            if (global == null) {
                throw new IllegalStateException("Global must be specified");
            }
            return new IndexResult(global, path, value, score, indexName);
        }
    }

    /**
     * Создает билдер для IndexResult
     *
     * @return новый билдер
     */
    public static Builder builder() {
        return new Builder();
    }
}