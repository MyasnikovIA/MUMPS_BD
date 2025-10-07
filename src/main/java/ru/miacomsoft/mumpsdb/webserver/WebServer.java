package ru.miacomsoft.mumpsdb.webserver;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.server.CommandParser;
import ru.miacomsoft.mumpsdb.server.MumpsCommandManager;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.json.JSONArray;

public class WebServer {
    private final Database database;
    private final MumpsCommandManager commandManager;
    private HttpServer server;
    private final int port;
    private final ConcurrentHashMap<String, ClientConnection> clients = new ConcurrentHashMap<>();
    private final ScheduledExecutorService broadcastScheduler = Executors.newScheduledThreadPool(1);

    public WebServer(Database database, int port) {
        this.database = database;
        this.commandManager = new MumpsCommandManager();
        this.commandManager.initialize(database);
        this.port = port;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        // Статические файлы
        server.createContext("/", new StaticFileHandler());

        // WebSocket-like соединение через EventSource
        server.createContext("/events", new EventSourceHandler());

        // API для выполнения команд
        server.createContext("/api/command", new CommandHandler());

        // API для метрик
        server.createContext("/api/metrics", new MetricsHandler());

        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        System.out.println("Web server started on port " + port);
        System.out.println("Open http://localhost:" + port + " in your browser");

        // Периодическая рассылка метрик
        startMetricsBroadcast();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        broadcastScheduler.shutdown();
        clients.clear();
    }

    private void startMetricsBroadcast() {
        broadcastScheduler.scheduleAtFixedRate(() -> {
            if (!clients.isEmpty()) {
                String metrics = getMetricsJson();
                broadcastToAll("metrics", metrics);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void broadcastToAll(String type, String data) {
        String message = formatEventMessage(type, data);
        clients.values().forEach(client -> {
            try {
                client.sendMessage(message);
            } catch (IOException e) {
                System.err.println("Failed to send message to client: " + e.getMessage());
                clients.remove(client.getClientId());
            }
        });
    }

    private String formatEventMessage(String type, String data) {
        return "event: " + type + "\n" +
                "data: " + data + "\n\n";
    }

    private String getMetricsJson() {
        try {
            JSONObject metrics = new JSONObject();
            metrics.put("timestamp", System.currentTimeMillis());
            metrics.put("clients", clients.size());
            metrics.put("status", "running");
            return metrics.toString();
        } catch (Exception e) {
            return "{\"error\": \"Failed to get metrics\"}";
        }
    }

    private class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/")) {
                path = "/index.html";
            }

            InputStream resourceStream = getClass().getResourceAsStream("/web" + path);
            if (resourceStream == null) {
                send404(exchange);
                return;
            }

            String contentType = getContentType(path);
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, 0);

            try (OutputStream os = exchange.getResponseBody()) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = resourceStream.read(buffer)) != -1) {
                    os.write(buffer, 0, bytesRead);
                }
            }
        }

        private String getContentType(String path) {
            if (path.endsWith(".html")) return "text/html";
            if (path.endsWith(".css")) return "text/css";
            if (path.endsWith(".js")) return "application/javascript";
            if (path.endsWith(".png")) return "image/png";
            return "text/plain";
        }

        private void send404(HttpExchange exchange) throws IOException {
            String response = "404 Not Found";
            exchange.sendResponseHeaders(404, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    private class EventSourceHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET");
            exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Cache-Control");

            exchange.sendResponseHeaders(200, 0);

            String clientId = exchange.getRemoteAddress().toString() + "-" + System.currentTimeMillis();
            ClientConnection client = new ClientConnection(clientId, exchange);
            clients.put(clientId, client);

            System.out.println("Client connected: " + clientId);

            // Отправляем приветственное сообщение
            JSONObject welcomeMsg = new JSONObject();
            welcomeMsg.put("clientId", clientId);
            welcomeMsg.put("message", "Connected to MUMPS DB Server");
            client.sendMessage(formatEventMessage("connected", welcomeMsg.toString()));

            // Держим соединение открытым
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(30000); // Keep-alive
                    client.sendMessage(": keep-alive\n\n");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                clients.remove(clientId);
                System.out.println("Client disconnected: " + clientId);
            }
        }
    }

    private class CommandHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
            exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            try {
                String requestBody = readRequestBody(exchange);
                JSONObject json = new JSONObject(requestBody);
                String command = json.getString("command");

                CommandParser.Command parsedCommand = CommandParser.parse(command);
                String result = commandManager.executeCommand(parsedCommand);

                JSONObject response = new JSONObject();
                response.put("success", true);
                response.put("result", result);
                response.put("timestamp", System.currentTimeMillis());

                sendJsonResponse(exchange, 200, response.toString());

            } catch (Exception e) {
                JSONObject error = new JSONObject();
                error.put("success", false);
                error.put("error", e.getMessage());
                error.put("timestamp", System.currentTimeMillis());

                sendJsonResponse(exchange, 400, error.toString());
            }
        }

        private String readRequestBody(HttpExchange exchange) throws IOException {
            try (InputStream is = exchange.getRequestBody();
                 ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    bos.write(buffer, 0, bytesRead);
                }
                return bos.toString(StandardCharsets.UTF_8.name());
            }
        }

        private void sendJsonResponse(HttpExchange exchange, int statusCode, String json) throws IOException {
            byte[] response = json.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }

        private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
            JSONObject error = new JSONObject();
            error.put("success", false);
            error.put("error", message);
            sendJsonResponse(exchange, statusCode, error.toString());
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");

            try {
                java.util.Map<String, Object> metrics = database.getDetailedStats();
                JSONObject json = new JSONObject(metrics);
                sendJsonResponse(exchange, 200, json.toString());
            } catch (Exception e) {
                sendError(exchange, 500, "Failed to get metrics: " + e.getMessage());
            }
        }

        private void sendJsonResponse(HttpExchange exchange, int statusCode, String json) throws IOException {
            byte[] response = json.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }

        private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
            JSONObject error = new JSONObject();
            error.put("success", false);
            error.put("error", message);
            sendJsonResponse(exchange, statusCode, error.toString());
        }
    }

    private static class ClientConnection {
        private final String clientId;
        private final HttpExchange exchange;
        private final OutputStream outputStream;

        ClientConnection(String clientId, HttpExchange exchange) throws IOException {
            this.clientId = clientId;
            this.exchange = exchange;
            this.outputStream = exchange.getResponseBody();
        }

        void sendMessage(String message) throws IOException {
            outputStream.write(message.getBytes(StandardCharsets.UTF_8));
            outputStream.flush();
        }

        String getClientId() {
            return clientId;
        }
    }
}