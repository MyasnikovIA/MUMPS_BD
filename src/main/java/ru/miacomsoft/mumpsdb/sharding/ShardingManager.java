
package ru.miacomsoft.mumpsdb.sharding;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Менеджер шардинга для распределенного хранения данных
 */
public class ShardingManager {
    private final List<ShardNode> shardNodes = new ArrayList<>();
    private final Map<String, String> globalToShardMap = new ConcurrentHashMap<>();
    private final ShardStrategy shardStrategy;
    private final boolean enabled;

    public ShardingManager(boolean enabled) {
        this.enabled = enabled;
        this.shardStrategy = new ConsistentHashStrategy();
    }

    /**
     * Добавление шарда в кластер
     */
    public void addShardNode(String nodeId, String host, int port, int weight) {
        ShardNode node = new ShardNode(nodeId, host, port, weight);
        shardNodes.add(node);
        shardStrategy.addNode(node);

        System.out.println("Added shard node: " + nodeId + " at " + host + ":" + port);
    }

    /**
     * Определение шарда для глобала
     */
    public String determineShard(String global) {
        if (!enabled || shardNodes.isEmpty()) {
            return "local"; // Локальный шард по умолчанию
        }

        String shardId = globalToShardMap.get(global);
        if (shardId == null) {
            shardId = shardStrategy.getShard(global);
            globalToShardMap.put(global, shardId);
        }

        return shardId;
    }

    /**
     * Получение узла шарда по ID
     */
    public ShardNode getShardNode(String shardId) {
        return shardNodes.stream()
                .filter(node -> node.getNodeId().equals(shardId))
                .findFirst()
                .orElse(null);
    }

    /**
     * Распределение операции по шардам
     */
    public <T> Map<String, List<T>> distributeOperation(List<T> items, ShardFunction<T> function) {
        Map<String, List<T>> distribution = new HashMap<>();

        for (T item : items) {
            String shardId = function.getShard(item);
            distribution.computeIfAbsent(shardId, k -> new ArrayList<>()).add(item);
        }

        return distribution;
    }

    /**
     * Проверка здоровья шардов
     */
    public Map<String, Boolean> healthCheck() {
        Map<String, Boolean> healthStatus = new HashMap<>();

        for (ShardNode node : shardNodes) {
            boolean healthy = checkNodeHealth(node);
            healthStatus.put(node.getNodeId(), healthy);
        }

        return healthStatus;
    }

    private boolean checkNodeHealth(ShardNode node) {
        // Реализация проверки доступности узла
        try {
            // Здесь может быть HTTP запрос или TCP соединение
            return true; // Заглушка
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Ребалансировка шардов
     */
    public void rebalanceShards() {
        if (!enabled) return;

        System.out.println("Starting shard rebalancing...");
        // Логика ребалансировки данных между шардами
        shardStrategy.rebalance(shardNodes);
    }

    // Getters
    public List<ShardNode> getShardNodes() {
        return new ArrayList<>(shardNodes);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getShardCount() {
        return shardNodes.size();
    }
}

/**
 * Узел шарда
 */
class ShardNode {
    private final String nodeId;
    private final String host;
    private final int port;
    private final int weight;
    private final long addedAt;
    private boolean active;

    public ShardNode(String nodeId, String host, int port, int weight) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.weight = weight;
        this.addedAt = System.currentTimeMillis();
        this.active = true;
    }

    // Getters
    public String getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getWeight() {
        return weight;
    }

    public long getAddedAt() {
        return addedAt;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public String toString() {
        return String.format("ShardNode{id=%s, host=%s, port=%d, weight=%d}",
                nodeId, host, port, weight);
    }
}

/**
 * Стратегия шардинга
 */
interface ShardStrategy {
    String getShard(String key);

    void addNode(ShardNode node);

    void removeNode(String nodeId);

    void rebalance(List<ShardNode> nodes);
}

/**
 * Consistent Hashing стратегия
 */
class ConsistentHashStrategy implements ShardStrategy {
    private final TreeMap<Integer, ShardNode> circle = new TreeMap<>();
    private final int virtualNodesPerNode = 100;

    @Override
    public String getShard(String key) {
        if (circle.isEmpty()) {
            return "local";
        }

        int hash = getHash(key);
        Map.Entry<Integer, ShardNode> entry = circle.ceilingEntry(hash);
        if (entry == null) {
            entry = circle.firstEntry();
        }

        return entry.getValue().getNodeId();
    }

    @Override
    public void addNode(ShardNode node) {
        for (int i = 0; i < virtualNodesPerNode; i++) {
            String virtualNodeKey = node.getNodeId() + "#" + i;
            int hash = getHash(virtualNodeKey);
            circle.put(hash, node);
        }
    }

    @Override
    public void removeNode(String nodeId) {
        circle.entrySet().removeIf(entry -> entry.getValue().getNodeId().equals(nodeId));
    }

    @Override
    public void rebalance(List<ShardNode> nodes) {
        circle.clear();
        for (ShardNode node : nodes) {
            addNode(node);
        }
    }

    private int getHash(String key) {
        return Math.abs(key.hashCode());
    }
}

/**
 * Функция для определения шарда
 */
interface ShardFunction<T> {
    String getShard(T item);
}