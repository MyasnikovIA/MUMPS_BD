package ru.miacomsoft.mumpsdb.replication;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.monitoring.MetricsCollector;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReplicationManager {
    private final Database localDatabase;
    private final List<ReplicaNode> replicas = new CopyOnWriteArrayList<>();
    private final ExecutorService replicationExecutor;
    private final MetricsCollector metrics;
    private ReplicationMode mode = ReplicationMode.ASYNC;

    public enum ReplicationMode {
        SYNC, ASYNC
    }

    public ReplicationManager(Database database) {
        this.localDatabase = database;
        this.replicationExecutor = Executors.newFixedThreadPool(5);
        this.metrics = MetricsCollector.getInstance();
    }

    public void addReplica(String nodeId, String host, int port) {
        ReplicaNode replica = new ReplicaNode(nodeId, host, port);
        replicas.add(replica);
        metrics.incrementCounter("replicas_added");
        System.out.println("Added replica: " + nodeId + " at " + host + ":" + port);
    }

    public void replicateSet(String global, Object value, Object... path) {
        if (replicas.isEmpty()) return;

        ReplicationTask task = new ReplicationTask("SET", global, value, path);

        if (mode == ReplicationMode.SYNC) {
            replicateSync(task);
        } else {
            replicateAsync(task);
        }
    }

    public void replicateKill(String global, Object... path) {
        if (replicas.isEmpty()) return;

        ReplicationTask task = new ReplicationTask("KILL", global, null, path);

        if (mode == ReplicationMode.SYNC) {
            replicateSync(task);
        } else {
            replicateAsync(task);
        }
    }

    private void replicateSync(ReplicationTask task) {
        long startTime = System.currentTimeMillis();
        List<ReplicaNode> successfulReplicas = new ArrayList<>();

        for (ReplicaNode replica : replicas) {
            try {
                if (replicateToNode(replica, task)) {
                    successfulReplicas.add(replica);
                }
            } catch (Exception e) {
                System.err.println("Replication failed to " + replica.nodeId + ": " + e.getMessage());
            }
        }

        metrics.recordOperationTime("replication_sync", startTime);
        metrics.setGauge("successful_replicas", successfulReplicas.size());
    }

    private void replicateAsync(ReplicationTask task) {
        replicationExecutor.submit(() -> {
            long startTime = System.currentTimeMillis();
            int successCount = 0;

            for (ReplicaNode replica : replicas) {
                try {
                    if (replicateToNode(replica, task)) {
                        successCount++;
                    }
                } catch (Exception e) {
                    System.err.println("Async replication failed to " + replica.nodeId + ": " + e.getMessage());
                }
            }

            metrics.recordOperationTime("replication_async", startTime);
            metrics.setGauge("async_successful_replicas", successCount);
        });
    }

    /**
     * Реализация репликации на конкретный узел через HTTP API
     */
    private boolean replicateToNode(ReplicaNode replica, ReplicationTask task) {
        long startTime = System.currentTimeMillis();
        try {
            String url = "http://" + replica.host + ":" + replica.port + "/api/replicate";

            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(10000);
            connection.setDoOutput(true);

            // Формируем JSON запрос
            String jsonRequest = buildReplicationJson(task);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonRequest.getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                // Читаем ответ для проверки
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(connection.getInputStream(), "utf-8"))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }

                    metrics.incrementCounter("replication_success");
                    metrics.recordOperationTime("replication_node_" + replica.nodeId, startTime);

                    System.out.println("Successfully replicated to " + replica.nodeId +
                            ": " + task.operation + " " + task.global);
                    return true;
                }
            } else {
                // Ошибка HTTP
                System.err.println("Replication to " + replica.nodeId + " failed with HTTP " + responseCode);
                metrics.incrementCounter("replication_http_errors");
                return false;
            }

        } catch (java.net.ConnectException e) {
            System.err.println("Cannot connect to replica " + replica.nodeId + " at " +
                    replica.host + ":" + replica.port);
            metrics.incrementCounter("replication_connection_errors");
            return false;
        } catch (java.net.SocketTimeoutException e) {
            System.err.println("Timeout connecting to replica " + replica.nodeId);
            metrics.incrementCounter("replication_timeout_errors");
            return false;
        } catch (Exception e) {
            System.err.println("Replication error to " + replica.nodeId + ": " + e.getMessage());
            metrics.incrementCounter("replication_errors");
            return false;
        }
    }

    /**
     * Формирует JSON для репликации
     */
    private String buildReplicationJson(ReplicationTask task) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"operation\": \"").append(task.operation).append("\",");
        json.append("\"global\": \"").append(task.global).append("\",");
        json.append("\"timestamp\": ").append(task.timestamp).append(",");

        if (task.path != null && task.path.length > 0) {
            json.append("\"path\": [");
            for (int i = 0; i < task.path.length; i++) {
                if (i > 0) json.append(",");
                if (task.path[i] instanceof String) {
                    json.append("\"").append(escapeJson((String) task.path[i])).append("\"");
                } else {
                    json.append(task.path[i]);
                }
            }
            json.append("],");
        } else {
            json.append("\"path\": [],");
        }

        if (task.value != null) {
            if (task.value instanceof String) {
                json.append("\"value\": \"").append(escapeJson((String) task.value)).append("\"");
            } else {
                json.append("\"value\": ").append(task.value);
            }
        } else {
            json.append("\"value\": null");
        }

        json.append("}");
        return json.toString();
    }

    private String escapeJson(String str) {
        return str.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    public void setReplicationMode(ReplicationMode mode) {
        this.mode = mode;
        System.out.println("Replication mode set to: " + mode);
    }

    public List<ReplicaNode> getReplicas() {
        return new ArrayList<>(replicas);
    }

    public int getReplicaCount() {
        return replicas.size();
    }

    public void shutdown() {
        replicationExecutor.shutdown();
        try {
            if (!replicationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                replicationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            replicationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("Replication manager shutdown complete");
    }

    public static class ReplicaNode {
        final String nodeId;
        final String host;
        final int port;

        ReplicaNode(String nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return nodeId + "[" + host + ":" + port + "]";
        }
    }

    public static class ReplicationTask {
        final String operation;
        final String global;
        final Object value;
        final Object[] path;
        final long timestamp;

        ReplicationTask(String operation, String global, Object value, Object[] path) {
            this.operation = operation;
            this.global = global;
            this.value = value;
            this.path = path;
            this.timestamp = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return operation + " " + global + Arrays.toString(path) + " = " + value;
        }
    }
}