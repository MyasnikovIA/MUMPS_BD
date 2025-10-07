package ru.miacomsoft.mumpsdb.core;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Оптимизированный узел дерева для хранения глобалов MUMPS
 */
public class TreeNode implements Serializable {
    private static final long serialVersionUID = 2L;

    private Object data;

    // Используем ConcurrentSkipListMap для отсортированных ключей и быстрого доступа
    private final Map<Object, TreeNode> children = new ConcurrentSkipListMap<>();

    // Кэш для часто запрашиваемых путей
    private transient Map<String, Object> pathCache = new ConcurrentHashMap<>();

    public TreeNode() {
    }

    public TreeNode(Object data) {
        this.data = data;
    }

    /**
     * Оптимизированная установка узла с кэшированием
     */
    public void setNode(Object[] path, Object value) {
        setNode(path, 0, value);
        // Очищаем кэш при изменении
        pathCache.clear();
    }

    private void setNode(Object[] path, int index, Object value) {
        if (index == path.length) {
            this.data = value;
            return;
        }

        Object key = path[index];
        TreeNode child = children.computeIfAbsent(key, k -> new TreeNode());
        child.setNode(path, index + 1, value);
    }

    /**
     * Оптимизированное получение узла с кэшированием
     */
    public Object getNode(Object[] path) {
        String cacheKey = buildCacheKey(path);
        Object cached = pathCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }

        Object result = getNode(path, 0);
        if (result != null) {
            pathCache.put(cacheKey, result);
        }
        return result;
    }

    private Object getNode(Object[] path, int index) {
        if (index == path.length) {
            return data;
        }

        Object key = path[index];
        TreeNode child = children.get(key);
        return child != null ? child.getNode(path, index + 1) : null;
    }

    private String buildCacheKey(Object[] path) {
        StringBuilder key = new StringBuilder();
        for (Object p : path) {
            key.append(p).append(":");
        }
        return key.toString();
    }

    /**
     * Удаляет узел по указанному пути
     */
    public void removeNode(Object[] path) {
        removeNode(path, 0);
        pathCache.clear();
    }

    private boolean removeNode(Object[] path, int index) {
        if (index == path.length) {
            data = null;
            return children.isEmpty();
        }

        Object key = path[index];
        TreeNode child = children.get(key);
        if (child == null) {
            return false;
        }

        boolean shouldRemoveChild = child.removeNode(path, index + 1);
        if (shouldRemoveChild) {
            children.remove(key);
        }

        return data == null && children.isEmpty();
    }

    /**
     * Выполняет запрос к дереву с указанной глубиной
     */
    public List<QueryResult> query(Object[] path, int depth) {
        List<QueryResult> results = new ArrayList<>();
        query(path, 0, new ArrayList<>(), depth, results);
        return results;
    }

    private void query(Object[] path, int index, List<Object> currentPath, int depth, List<QueryResult> results) {
        if (index >= path.length) {
            if (data != null) {
                results.add(new QueryResult(currentPath.toArray(), data));
            }
            if (depth > 0) {
                for (Map.Entry<Object, TreeNode> entry : children.entrySet()) {
                    List<Object> newPath = new ArrayList<>(currentPath);
                    newPath.add(entry.getKey());
                    entry.getValue().query(new Object[0], 0, newPath, depth - 1, results);
                }
            }
            return;
        }
        Object key = path[index];
        TreeNode child = children.get(key);
        if (child != null) {
            List<Object> newPath = new ArrayList<>(currentPath);
            newPath.add(key);
            child.query(path, index + 1, newPath, depth, results);
        }
    }

    /**
     * Оптимизированный подсчет узлов
     */
    public int countNodes() {
        int count = data != null ? 1 : 0;
        for (TreeNode child : children.values()) {
            count += child.countNodes();
        }
        return count;
    }

    /**
     * Получает данные узла
     */
    public Object getData() {
        return data;
    }

    /**
     * Устанавливает данные узла
     */
    public void setData(Object data) {
        this.data = data;
        pathCache.clear();
    }

    /**
     * Получает map дочерних узлов
     */
    public Map<Object, TreeNode> getChildren() {
        return children;
    }

    /**
     * Быстрое получение всех дочерних ключей
     */
    public Set<Object> getChildKeys() {
        return children.keySet();
    }

    /**
     * Проверяет, является ли узел листом (нет дочерних узлов)
     */
    public boolean isLeaf() {
        return children.isEmpty();
    }

    /**
     * Проверяет, пустой ли узел (нет данных и дочерних узлов)
     */
    public boolean isEmpty() {
        return data == null && children.isEmpty();
    }

    /**
     * Получает все пути и значения в поддереве
     */
    public Map<List<Object>, Object> getAllPaths() {
        Map<List<Object>, Object> paths = new HashMap<>();
        getAllPaths(new ArrayList<>(), paths);
        return paths;
    }

    private void getAllPaths(List<Object> currentPath, Map<List<Object>, Object> paths) {
        if (data != null) {
            paths.put(new ArrayList<>(currentPath), data);
        }
        for (Map.Entry<Object, TreeNode> entry : children.entrySet()) {
            List<Object> newPath = new ArrayList<>(currentPath);
            newPath.add(entry.getKey());
            entry.getValue().getAllPaths(newPath, paths);
        }
    }

    /**
     * Быстрый поиск по значению в поддереве
     */
    public List<Map.Entry<List<Object>, Object>> findValues(Object targetValue) {
        List<Map.Entry<List<Object>, Object>> results = new ArrayList<>();
        findValues(new ArrayList<>(), targetValue, results);
        return results;
    }

    private void findValues(List<Object> currentPath, Object targetValue,
                            List<Map.Entry<List<Object>, Object>> results) {
        if (data != null && data.equals(targetValue)) {
            results.add(new AbstractMap.SimpleEntry<>(new ArrayList<>(currentPath), data));
        }
        for (Map.Entry<Object, TreeNode> entry : children.entrySet()) {
            List<Object> newPath = new ArrayList<>(currentPath);
            newPath.add(entry.getKey());
            entry.getValue().findValues(newPath, targetValue, results);
        }
    }

    /**
     * Быстрое получение всех путей с использованием Stream API
     */
    public List<List<Object>> getAllPathsFast() {
        List<List<Object>> paths = new ArrayList<>();
        getAllPathsFast(new ArrayList<>(), paths);
        return paths;
    }

    private void getAllPathsFast(List<Object> currentPath, List<List<Object>> paths) {
        if (data != null) {
            paths.add(new ArrayList<>(currentPath));
        }
        for (Map.Entry<Object, TreeNode> entry : children.entrySet()) {
            List<Object> newPath = new ArrayList<>(currentPath);
            newPath.add(entry.getKey());
            entry.getValue().getAllPathsFast(newPath, paths);
        }
    }

    /**
     * Очищает узел и все дочерние узлы
     */
    public void clear() {
        data = null;
        children.clear();
        pathCache.clear();
    }

    @Override
    public String toString() {
        return "TreeNode{" +
                "data=" + data +
                ", children=" + children.keySet() +
                '}';
    }

    /**
     * Создает глубокую копию узла
     */
    public TreeNode deepCopy() {
        TreeNode copy = new TreeNode(data);
        for (Map.Entry<Object, TreeNode> entry : children.entrySet()) {
            copy.children.put(entry.getKey(), entry.getValue().deepCopy());
        }
        return copy;
    }
}