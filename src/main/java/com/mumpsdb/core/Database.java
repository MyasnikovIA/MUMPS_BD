package com.mumpsdb.core;

import com.mumpsdb.ConfigLoader;
import com.mumpsdb.embedding.EmbeddingService;
import com.mumpsdb.embedding.EmbeddingStorage;
import com.mumpsdb.embedding.VectorSearchResult;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Database {
    private final Map<String, TreeNode> globalStorage = new ConcurrentHashMap<>();
    private final ReadWriteLock storageLock = new ReentrantReadWriteLock();
    private final ThreadLocal<Transaction> currentTransaction = new ThreadLocal<>();

    // Кэш для часто запрашиваемых путей
    private final Map<String, Object> queryCache = new ConcurrentHashMap<>();
    private final int MAX_CACHE_SIZE = 10000;

    // Индекс для быстрого поиска
    private final Map<String, Set<String>> valueIndex = new ConcurrentHashMap<>();
    private final Map<String, Map<Object, Set<String>>> pathValueIndex = new ConcurrentHashMap<>();

    // Новые поля для работы с embedding
    private final EmbeddingService embeddingService;
    private final EmbeddingStorage embeddingStorage;
    private boolean autoEmbeddingEnabled;
    private final ConfigLoader configLoader;

    public Database() {
        this.configLoader = new ConfigLoader();
        this.autoEmbeddingEnabled = configLoader.isAutoEmbeddingEnabled();
        this.embeddingService = new EmbeddingService();
        this.embeddingStorage = new EmbeddingStorage(embeddingService);

        // Запуск очистки кэша
        startCacheCleanup();
    }

    /**
     * Оптимизированный метод SET с индексацией и кэшированием
     */
    public void set(String global, Object value, Object... path) {
        validateGlobalName(global);
        String globalName = normalizeGlobalName(global);
        Object[] normalizedPath = normalizePathTypes(path);

        String cacheKey = buildCacheKey(globalName, normalizedPath);

        Transaction transaction = currentTransaction.get();
        if (transaction != null) {
            transaction.set(globalName, normalizedPath, value);
        } else {
            storageLock.writeLock().lock();
            try {
                TreeNode tree = globalStorage.computeIfAbsent(globalName, k -> new TreeNode());
                tree.setNode(normalizedPath, value);

                // Обновляем кэш
                queryCache.put(cacheKey, value);

                // Обновляем индексы
                updateIndexes(globalName, normalizedPath, value);

                // Автоматически создаем embedding если включено
                if (autoEmbeddingEnabled && value != null) {
                    try {
                        float[] embedding = embeddingService.getEmbedding(value.toString());
                        embeddingStorage.storeEmbedding(globalName, normalizedPath, value, embedding);
                    } catch (Exception e) {
                        System.err.println("Failed to create embedding for " + globalName + " path: " + Arrays.toString(normalizedPath) + ": " + e.getMessage());
                    }
                }
            } finally {
                storageLock.writeLock().unlock();
            }
        }
    }

    /**
     * Оптимизированный метод GET с кэшированием
     */
    public Object get(String global, Object... path) {
        validateGlobalName(global);
        String globalName = normalizeGlobalName(global);

        // Проверяем кэш
        String cacheKey = buildCacheKey(globalName, path);
        Object cachedValue = queryCache.get(cacheKey);
        if (cachedValue != null) {
            return cachedValue;
        }

        storageLock.readLock().lock();
        try {
            TreeNode tree = globalStorage.get(globalName);
            Object result = tree != null ? tree.getNode(path) : null;

            // Кэшируем результат
            if (result != null) {
                queryCache.put(cacheKey, result);
            }

            return result;
        } finally {
            storageLock.readLock().unlock();
        }
    }

    /**
     * Быстрый поиск по значению с использованием индекса
     */
    public List<SearchResult> fastSearch(String value) {
        Set<String> globals = valueIndex.get(value);
        if (globals == null) {
            return Collections.emptyList();
        }

        List<SearchResult> results = new ArrayList<>();
        storageLock.readLock().lock();
        try {
            for (String global : globals) {
                TreeNode tree = globalStorage.get(global);
                if (tree != null) {
                    // Получаем все пути для этого значения
                    Map<List<Object>, Object> allPaths = tree.getAllPaths();
                    for (Map.Entry<List<Object>, Object> entry : allPaths.entrySet()) {
                        if (value.equals(entry.getValue().toString())) {
                            results.add(new SearchResult(global, entry.getKey().toArray(), entry.getValue()));
                        }
                    }
                }
            }
        } finally {
            storageLock.readLock().unlock();
        }
        return results;
    }

    /**
     * Обновление индексов для быстрого поиска
     */
    private void updateIndexes(String global, Object[] path, Object value) {
        if (value == null) return;

        String valueStr = value.toString();

        // Обновляем индекс значений
        valueIndex.computeIfAbsent(valueStr, k -> ConcurrentHashMap.newKeySet())
                .add(global);

        // Обновляем индекс путей и значений
        String pathKey = buildPathKey(path);
        pathValueIndex.computeIfAbsent(global, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(valueStr, k -> ConcurrentHashMap.newKeySet())
                .add(pathKey);
    }

    private String buildCacheKey(String global, Object[] path) {
        StringBuilder key = new StringBuilder(global);
        for (Object p : path) {
            key.append(":").append(p);
        }
        return key.toString();
    }

    private String buildPathKey(Object[] path) {
        return Arrays.stream(path)
                .map(Object::toString)
                .collect(Collectors.joining(":"));
    }

    /**
     * Запуск периодической очистки кэша
     */
    private void startCacheCleanup() {
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                cleanupCache();
            }
        }, 300000, 300000); // Каждые 5 минут
    }

    private void cleanupCache() {
        if (queryCache.size() > MAX_CACHE_SIZE) {
            // Удаляем 20% самых старых записей
            int entriesToRemove = MAX_CACHE_SIZE / 5;
            Iterator<String> iterator = queryCache.keySet().iterator();
            for (int i = 0; i < entriesToRemove && iterator.hasNext(); i++) {
                iterator.next();
                iterator.remove();
            }
        }
    }

    // Вспомогательный класс для результатов поиска
    public static class SearchResult {
        private final String global;
        private final Object[] path;
        private final Object value;

        public SearchResult(String global, Object[] path, Object value) {
            this.global = global;
            this.path = path;
            this.value = value;
        }

        // Getters
        public String getGlobal() { return global; }
        public Object[] getPath() { return path; }
        public Object getValue() { return value; }
    }

    // Остальные методы остаются без изменений...
    public void kill(String global, Object... path) {
        validateGlobalName(global);
        String globalName = normalizeGlobalName(global);

        Transaction transaction = currentTransaction.get();
        if (transaction != null) {
            transaction.kill(globalName, path);
        } else {
            storageLock.writeLock().lock();
            try {
                if (path.length == 0) {
                    globalStorage.remove(globalName);
                    if (autoEmbeddingEnabled) {
                        embeddingStorage.removeAllEmbeddings(globalName);
                    }
                } else {
                    TreeNode tree = globalStorage.get(globalName);
                    if (tree != null) {
                        tree.removeNode(path);
                        if (autoEmbeddingEnabled) {
                            embeddingStorage.removeEmbedding(globalName, path);
                        }
                    }
                }
            } finally {
                storageLock.writeLock().unlock();
            }
        }
    }

    public List<QueryResult> query(String global, Object[] path, int depth) {
        validateGlobalName(global);
        String globalName = normalizeGlobalName(global);

        storageLock.readLock().lock();
        try {
            TreeNode tree = globalStorage.get(globalName);
            return tree != null ? tree.query(path, depth) : Collections.emptyList();
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public boolean exists(String global, Object... path) {
        return get(global, path) != null;
    }

    public List<String> getGlobalNames() {
        storageLock.readLock().lock();
        try {
            return new ArrayList<>(globalStorage.keySet());
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public List<String> getGlobalNodes(String globalWithPath) {
        validateGlobalName(globalWithPath);
        String normalizedGlobal = normalizeGlobalName(globalWithPath);
        ParsedGlobal parsed = parseGlobalName(normalizedGlobal);
        String global = parsed.globalName;
        Object[] path = parsed.path;

        storageLock.readLock().lock();
        try {
            String storageKey = "^" + global;
            TreeNode tree = globalStorage.get(storageKey);
            if (tree == null) {
                return Collections.emptyList();
            }

            TreeNode targetNode = tree;
            if (path.length > 0) {
                targetNode = getTreeNodeByPath(tree, path);
                if (targetNode == null) {
                    return Collections.emptyList();
                }
            }

            List<String> nodes = new ArrayList<>();
            collectAllNodes(global, targetNode, new ArrayList<>(Arrays.asList(path)), nodes);
            return nodes;
        } finally {
            storageLock.readLock().unlock();
        }
    }

    private void collectAllNodes(String globalName, TreeNode node, List<Object> currentPath, List<String> nodes) {
        StringBuilder nodeBuilder = new StringBuilder();
        nodeBuilder.append("^").append(globalName);

        if (!currentPath.isEmpty()) {
            nodeBuilder.append("(");
            for (int i = 0; i < currentPath.size(); i++) {
                if (i > 0) {
                    nodeBuilder.append(",");
                }
                Object pathElement = currentPath.get(i);
                if (pathElement instanceof String) {
                    nodeBuilder.append("\"").append(pathElement).append("\"");
                } else {
                    nodeBuilder.append(pathElement);
                }
            }
            nodeBuilder.append(")");
        }

        if (node.getData() != null) {
            nodeBuilder.append("=");
            if (node.getData() instanceof String) {
                nodeBuilder.append("\"").append(node.getData().toString()).append("\"");
            } else {
                nodeBuilder.append(node.getData().toString());
            }
        }

        nodes.add(nodeBuilder.toString());

        for (Map.Entry<Object, TreeNode> entry : node.getChildren().entrySet()) {
            List<Object> newPath = new ArrayList<>(currentPath);
            newPath.add(entry.getKey());
            collectAllNodes(globalName, entry.getValue(), newPath, nodes);
        }
    }

    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        storageLock.readLock().lock();
        try {
            stats.put("globalCount", globalStorage.size());
            stats.put("totalNodes", countAllNodes());
            stats.put("memoryUsage", estimateMemoryUsage());
            stats.put("embeddingCount", getEmbeddingCount());
            stats.put("autoEmbeddingEnabled", autoEmbeddingEnabled);
            stats.put("cacheSize", queryCache.size());
            stats.put("indexSize", valueIndex.size());
            return stats;
        } finally {
            storageLock.readLock().unlock();
        }
    }

    public Transaction beginTransaction() {
        if (currentTransaction.get() != null) {
            throw new IllegalStateException("Transaction already in progress");
        }
        Transaction transaction = new Transaction(this);
        currentTransaction.set(transaction);
        return transaction;
    }

    public void commitTransaction(Transaction transaction) {
        if (currentTransaction.get() != transaction) {
            throw new IllegalStateException("Invalid transaction");
        }
        storageLock.writeLock().lock();
        try {
            transaction.commitTo(this);
            currentTransaction.remove();
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public void rollbackTransaction() {
        currentTransaction.remove();
    }

    // Embedding методы
    public void setWithEmbedding(String global, Object value, float[] embedding, Object... path) {
        if (!autoEmbeddingEnabled) {
            set(global, value, path);
            return;
        }
        validateGlobalName(global);
        String globalName = normalizeGlobalName(global);
        storageLock.writeLock().lock();
        try {
            globalStorage.computeIfAbsent(globalName, k -> new TreeNode()).setNode(path, value);
            embeddingStorage.storeEmbedding(globalName, path, value, embedding);
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public List<VectorSearchResult> similaritySearch(String query, int topK) {
        if (!autoEmbeddingEnabled) {
            return Collections.emptyList();
        }
        try {
            return embeddingStorage.similaritySearch(query, topK);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public List<VectorSearchResult> similaritySearch(String query, int topK, String global) {
        if (!autoEmbeddingEnabled) {
            return Collections.emptyList();
        }
        try {
            return embeddingStorage.similaritySearch(query, topK, global);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public List<VectorSearchResult> exactSearch(String query) {
        if (!autoEmbeddingEnabled) {
            return Collections.emptyList();
        }
        return embeddingStorage.exactSearch(query, null);
    }

    public List<VectorSearchResult> exactSearch(String query, String global) {
        if (!autoEmbeddingEnabled) {
            return Collections.emptyList();
        }
        return embeddingStorage.exactSearch(query, global);
    }

    public void setAutoEmbeddingEnabled(boolean enabled) {
        this.autoEmbeddingEnabled = enabled;
    }

    public boolean isAutoEmbeddingEnabled() {
        return autoEmbeddingEnabled;
    }

    public int getEmbeddingCount() {
        if (!autoEmbeddingEnabled) {
            return 0;
        }
        return embeddingStorage.getEmbeddingCount();
    }

    // Приватные вспомогательные методы
    private int countAllNodes() {
        int count = 0;
        for (TreeNode tree : globalStorage.values()) {
            count += tree.countNodes();
        }
        return count;
    }

    private long estimateMemoryUsage() {
        return countAllNodes() * 100L;
    }

    private String normalizeGlobalName(String global) {
        return global.startsWith("^") ? global : "^" + global;
    }

    private void validateGlobalName(String global) {
        if (global == null || global.trim().isEmpty()) {
            throw new IllegalArgumentException("Global name cannot be null or empty");
        }
    }

    private Object[] normalizePathTypes(Object[] path) {
        if (path == null || path.length == 0) {
            return path;
        }
        Object[] normalized = new Object[path.length];
        for (int i = 0; i < path.length; i++) {
            Object element = path[i];
            if (element instanceof Integer) {
                normalized[i] = ((Integer) element).longValue();
            } else if (element instanceof String) {
                String str = (String) element;
                try {
                    normalized[i] = Long.parseLong(str);
                } catch (NumberFormatException e) {
                    normalized[i] = str;
                }
            } else {
                normalized[i] = element;
            }
        }
        return normalized;
    }

    private TreeNode getTreeNodeByPath(TreeNode root, Object[] path) {
        TreeNode currentNode = root;
        for (int i = 0; i < path.length; i++) {
            Object pathElement = path[i];
            if (currentNode == null) {
                return null;
            }
            Map<Object, TreeNode> children = currentNode.getChildren();
            TreeNode nextNode = null;
            for (Map.Entry<Object, TreeNode> entry : children.entrySet()) {
                Object key = entry.getKey();
                if (Objects.equals(pathElement, key)) {
                    nextNode = entry.getValue();
                    break;
                }
            }
            currentNode = nextNode;
            if (currentNode == null) {
                return null;
            }
        }
        return currentNode;
    }

    // Вспомогательный класс для парсинга
    private static class ParsedGlobal {
        String globalName;
        Object[] path;
        ParsedGlobal(String globalName, Object[] path) {
            this.globalName = globalName;
            this.path = path;
        }
    }

    private ParsedGlobal parseGlobalName(String globalWithPath) {
        if (!globalWithPath.startsWith("^")) {
            return new ParsedGlobal(globalWithPath, new Object[0]);
        }
        String withoutCaret = globalWithPath.substring(1);
        int bracketIndex = withoutCaret.indexOf('(');
        if (bracketIndex == -1) {
            return new ParsedGlobal(withoutCaret, new Object[0]);
        }
        String globalName = withoutCaret.substring(0, bracketIndex);
        if (!withoutCaret.endsWith(")")) {
            return new ParsedGlobal(globalName, new Object[0]);
        }
        String pathString = withoutCaret.substring(bracketIndex + 1, withoutCaret.length() - 1);
        List<Object> pathElements = parsePathElements(pathString);
        return new ParsedGlobal(globalName, pathElements.toArray());
    }

    private List<Object> parsePathElements(String pathString) {
        List<Object> elements = new ArrayList<>();
        if (pathString == null || pathString.trim().isEmpty()) {
            return elements;
        }
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int bracketDepth = 0;
        for (int i = 0; i < pathString.length(); i++) {
            char c = pathString.charAt(i);
            if (c == '"' && bracketDepth == 0) {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == '(' && !inQuotes) {
                bracketDepth++;
                current.append(c);
            } else if (c == ')' && !inQuotes) {
                bracketDepth--;
                current.append(c);
            } else if (c == ',' && bracketDepth == 0 && !inQuotes) {
                parts.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            parts.add(current.toString().trim());
        }
        for (String part : parts) {
            Object element = parsePathElement(part);
            elements.add(element);
        }
        return elements;
    }

    private Object parsePathElement(String element) {
        String trimmed = element.trim();
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            return trimmed.substring(1, trimmed.length() - 1);
        } else {
            try {
                return Long.parseLong(trimmed);
            } catch (NumberFormatException e1) {
                try {
                    return Double.parseDouble(trimmed);
                } catch (NumberFormatException e2) {
                    return trimmed;
                }
            }
        }
    }

    // Методы для персистентности
    public Map<String, TreeNode> getGlobalStorage() {
        return new HashMap<>(globalStorage);
    }

    public void setGlobalStorage(Map<String, TreeNode> storage) {
        storageLock.writeLock().lock();
        try {
            globalStorage.clear();
            globalStorage.putAll(storage);
            if (autoEmbeddingEnabled) {
                new Thread(this::generateEmbeddingForExistingData).start();
            }
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    public List<String> getGlobalNodesZW(String globalWithPath) {
        validateGlobalName(globalWithPath);
        String normalizedGlobal = normalizeGlobalName(globalWithPath);
        ParsedGlobal parsed = parseGlobalName(normalizedGlobal);
        String global = parsed.globalName;
        Object[] path = parsed.path;

        storageLock.readLock().lock();
        try {
            String storageKey = "^" + global;
            TreeNode tree = globalStorage.get(storageKey);
            if (tree == null) {
                return Collections.emptyList();
            }
            TreeNode targetNode = getTreeNodeByPathZW(tree, path);
            if (targetNode == null) {
                return Collections.emptyList();
            }
            List<String> nodes = new ArrayList<>();
            collectAllNodesZW(global, targetNode, new ArrayList<>(Arrays.asList(path)), nodes);
            return nodes;
        } finally {
            storageLock.readLock().unlock();
        }
    }

    private TreeNode getTreeNodeByPathZW(TreeNode root, Object[] path) {
        TreeNode currentNode = root;
        Object[] normalizedPath = normalizePathTypes(path);
        for (Object pathElement : normalizedPath) {
            if (currentNode == null) {
                return null;
            }
            Map<Object, TreeNode> children = currentNode.getChildren();
            currentNode = children.get(pathElement);
            if (currentNode == null) {
                return null;
            }
        }
        return currentNode;
    }

    private void collectAllNodesZW(String globalName, TreeNode node, List<Object> currentPath, List<String> nodes) {
        if (node.getData() != null) {
            StringBuilder nodeBuilder = new StringBuilder();
            nodeBuilder.append("^").append(globalName);
            if (!currentPath.isEmpty()) {
                nodeBuilder.append("(");
                for (int i = 0; i < currentPath.size(); i++) {
                    if (i > 0) nodeBuilder.append(",");
                    Object pathElement = currentPath.get(i);
                    if (pathElement instanceof String) {
                        nodeBuilder.append("\"").append(pathElement).append("\"");
                    } else {
                        nodeBuilder.append(pathElement);
                    }
                }
                nodeBuilder.append(")");
            }
            nodeBuilder.append("=");
            if (node.getData() instanceof String) {
                nodeBuilder.append("\"").append(node.getData().toString()).append("\"");
            } else {
                nodeBuilder.append(node.getData().toString());
            }
            nodes.add(nodeBuilder.toString());
        }
        for (Map.Entry<Object, TreeNode> entry : node.getChildren().entrySet()) {
            List<Object> newPath = new ArrayList<>(currentPath);
            newPath.add(entry.getKey());
            collectAllNodesZW(globalName, entry.getValue(), newPath, nodes);
        }
    }

    public void generateEmbeddingForExistingData() {
        if (!autoEmbeddingEnabled) {
            return;
        }
        storageLock.writeLock().lock();
        try {
            for (Map.Entry<String, TreeNode> entry : globalStorage.entrySet()) {
                String global = entry.getKey();
                TreeNode tree = entry.getValue();
                generateEmbeddingForTreeNode(global, tree, new ArrayList<>());
            }
        } finally {
            storageLock.writeLock().unlock();
        }
    }

    private void generateEmbeddingForTreeNode(String global, TreeNode node, List<Object> currentPath) {
        if (node.getData() != null) {
            try {
                float[] embedding = embeddingService.getEmbedding(node.getData().toString());
                embeddingStorage.storeEmbedding(global, currentPath.toArray(), node.getData(), embedding);
            } catch (Exception e) {
                System.err.println("Failed to generate embedding for " + global + " path: " + currentPath + ": " + e.getMessage());
            }
        }
        for (Map.Entry<Object, TreeNode> entry : node.getChildren().entrySet()) {
            List<Object> newPath = new ArrayList<>(currentPath);
            newPath.add(entry.getKey());
            generateEmbeddingForTreeNode(global, entry.getValue(), newPath);
        }
    }
}