package ru.miacomsoft.mumpsdb;

import ru.miacomsoft.mumpsdb.benchmark.DatabaseBenchmark;
import ru.miacomsoft.mumpsdb.console.ConsoleCommandProcessor;
import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.persistence.AOFManager;
import ru.miacomsoft.mumpsdb.persistence.SnapshotService;
import ru.miacomsoft.mumpsdb.server.SocketServer;
import ru.miacomsoft.mumpsdb.webserver.WebServer;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final int DEFAULT_PORT = 9090;
    private static final String SNAPSHOT_FILE = "database.snapshot";
    private static final String AOF_FILE = "commands.aof";

    private Database database;
    private SocketServer socketServer;
    private WebServer webServer;
    private SnapshotService snapshotService;
    private AOFManager aofManager;
    private ScheduledExecutorService scheduler;
    private boolean socketMode = false;
    private boolean consoleMode = false;
    private volatile boolean running = true;

    public static void main(String[] args) {
        Main main = new Main();
        main.parseArguments(args);
        main.start();

        // Добавляем shutdown hook для graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(main::stop));
    }

    /**
     * Парсинг аргументов командной строки
     */
    private void parseArguments(String[] args) {
        if (args.length == 0) {
            // Режим по умолчанию - оба режима
            socketMode = true;
            consoleMode = true;
            return;
        }

        for (String arg : args) {
            switch (arg.toLowerCase()) {
                case "--socket":
                case "-s":
                    socketMode = true;
                    break;
                case "--console":
                case "-c":
                    consoleMode = true;
                    break;
                case "--both":
                case "-b":
                    socketMode = true;
                    consoleMode = true;
                    break;
                case "--help":
                case "-h":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    System.err.println("Unknown argument: " + arg);
                    printUsage();
                    System.exit(1);
            }
        }

        // Если не указаны режимы, используем оба
        if (!socketMode && !consoleMode) {
            socketMode = true;
            consoleMode = true;
        }
    }

    private void printUsage() {
        System.out.println("MUMPS-like Database Server");
        System.out.println("Usage: java -jar mumpsdb.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --socket, -s     Start in socket server mode only");
        System.out.println("  --console, -c    Start in console mode only");
        System.out.println("  --both, -b       Start in both modes (default)");
        System.out.println("  --help, -h       Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar mumpsdb.jar --socket    # Socket server only on port 9090");
        System.out.println("  java -jar mumpsdb.jar --console   # Console mode only");
        System.out.println("  java -jar mumpsdb.jar --both      # Both socket and console modes");
        System.out.println("  java -jar mumpsdb.jar             # Default: both modes");
    }

    public void start() {
        try {
            // Инициализация базы данных
            database = new Database();

            // Инициализация сервисов персистентности
            snapshotService = new SnapshotService(database, SNAPSHOT_FILE);
            aofManager = new AOFManager(AOF_FILE);

            // Загрузка снапшота
            try {
                snapshotService.loadSnapshot();
                System.out.println("Snapshot loaded successfully");
            } catch (Exception e) {
                System.out.println("No snapshot found, starting with empty database");
            }

            // Запуск AOF manager
            try {
                aofManager.start();
                System.out.println("AOF manager started");
            } catch (Exception e) {
                System.out.println("Failed to start AOF manager: " + e.getMessage());
            }

            // Настройка периодического сохранения
            scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(this::periodicSnapshot, 5, 5, TimeUnit.MINUTES);

            // Запуск бенчмарка при старте (опционально)
            if (shouldRunBenchmark()) {
                runStartupBenchmark();
            }

            // Запуск выбранных режимов
            if (socketMode) {
                startSocketServer();
            }

            // Запуск веб-сервера
            startWebServer();

            if (consoleMode) {
                startConsoleMode();
            } else {
                // Если только socket режим, ждем завершения
                waitForShutdown();
            }

        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
            stop();
        }
    }

    private void startSocketServer() {
        try {
            socketServer = new SocketServer(DEFAULT_PORT, database);

            Thread serverThread = new Thread(() -> {
                try {
                    System.out.println("Socket server starting on port " + DEFAULT_PORT);
                    socketServer.start();
                } catch (IOException e) {
                    System.err.println("Socket server error: " + e.getMessage());
                }
            });
            serverThread.setDaemon(true);
            serverThread.start();

            System.out.println("Socket server started on port " + DEFAULT_PORT);
            System.out.println("Connect using: telnet localhost " + DEFAULT_PORT);

        } catch (Exception e) {
            System.err.println("Failed to start socket server: " + e.getMessage());
        }
    }

    private void startWebServer() {
        try {
            ConfigLoader configLoader = new ConfigLoader();
            int webPort = Integer.parseInt(configLoader.getProperties()
                    .getProperty("webserver.port", "8080"));
            boolean webEnabled = Boolean.parseBoolean(configLoader.getProperties()
                    .getProperty("webserver.enabled", "true"));

            if (webEnabled) {
                webServer = new WebServer(database, webPort);
                webServer.start();
                System.out.println("Web server started on port " + webPort);
                System.out.println("Open http://localhost:" + webPort + " in your browser");
            } else {
                System.out.println("Web server is disabled in configuration");
            }
        } catch (Exception e) {
            System.err.println("Failed to start web server: " + e.getMessage());
        }
    }

    private void startConsoleMode() {
        try {
            Thread consoleThread = new Thread(() -> {
                System.out.println("Console mode starting...");
                ConsoleCommandProcessor consoleProcessor =
                        new ConsoleCommandProcessor(database);
                consoleProcessor.start();
                // Когда консоль завершится, останавливаем сервер
                stop();
            });
            consoleThread.setDaemon(false);
            consoleThread.start();

        } catch (Exception e) {
            System.err.println("Failed to start console mode: " + e.getMessage());
        }
    }

    /**
     * Ожидание команды завершения в socket-only режиме
     */
    private void waitForShutdown() {
        Scanner scanner = new Scanner(System.in);
        System.out.println();
        System.out.println("Server running in socket mode. Press 'q' + Enter to shutdown.");

        while (running) {
            try {
                if (scanner.hasNextLine()) {
                    String input = scanner.nextLine().trim().toLowerCase();
                    if ("q".equals(input) || "quit".equals(input) || "exit".equals(input)) {
                        System.out.println("Shutdown initiated...");
                        break;
                    }
                } else {
                    // Нет ввода, ждем немного
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                break;
            }
        }

        scanner.close();
        stop();
    }

    private boolean shouldRunBenchmark() {
        return Boolean.parseBoolean(
                new ConfigLoader().getProperties()
                        .getProperty("benchmark.on.startup", "false")
        );
    }

    private void runStartupBenchmark() {
        System.out.println("Running startup benchmark...");
        DatabaseBenchmark benchmark = new DatabaseBenchmark(database);
        DatabaseBenchmark.BenchmarkResult result = benchmark.runComprehensiveBenchmark();
        System.out.println(result.toString());
        benchmark.shutdown();
    }

    public void stop() {
        if (!running) return;
        running = false;

        System.out.println("Shutting down server...");

        try {
            // Сохраняем финальный снапшот
            if (snapshotService != null) {
                snapshotService.saveSnapshot();
                System.out.println("Final snapshot saved");
            }
        } catch (Exception e) {
            System.err.println("Error saving final snapshot: " + e.getMessage());
        }

        // Останавливаем веб-сервер
        if (webServer != null) {
            webServer.stop();
            System.out.println("Web server stopped");
        }

        // Останавливаем socket server
        if (socketServer != null) {
            socketServer.stop();
            System.out.println("Socket server stopped");
        }

        // Останавливаем AOF manager
        if (aofManager != null) {
            aofManager.stop();
            System.out.println("AOF manager stopped");
        }

        // Останавливаем scheduler
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                System.out.println("Scheduler stopped");
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        System.out.println("Server shutdown complete");
    }

    private void periodicSnapshot() {
        try {
            if (snapshotService != null) {
                snapshotService.saveSnapshot();
            }
        } catch (Exception e) {
            System.err.println("Periodic snapshot failed: " + e.getMessage());
        }
    }
}