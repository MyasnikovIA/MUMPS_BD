package com.mumpsdb.console;

import com.mumpsdb.core.Database;
import com.mumpsdb.persistence.AOFManager;
import com.mumpsdb.persistence.SnapshotService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Сервер для работы в консольном режиме
 */
public class ConsoleServer {
    private final Database database;
    private final ConsoleCommandProcessor consoleProcessor;
    private final SnapshotService snapshotService;
    private final AOFManager aofManager;
    private final ScheduledExecutorService scheduler;

    private static final String SNAPSHOT_FILE = "database.snapshot";
    private static final String AOF_FILE = "commands.aof";

    public ConsoleServer() {
        this.database = new Database();
        this.consoleProcessor = new ConsoleCommandProcessor(database);
        this.snapshotService = new SnapshotService(database, SNAPSHOT_FILE);
        this.aofManager = new AOFManager(AOF_FILE);
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public void start() {
        try {
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
                System.out.println("AOF manager failed to start: " + e.getMessage());
            }

            // Настройка периодического сохранения
            scheduler.scheduleAtFixedRate(this::periodicSnapshot, 5, 5, TimeUnit.MINUTES);
            System.out.println("Periodic snapshot scheduler started");

            // Добавляем shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

            // Запускаем консольный процессор
            consoleProcessor.start();

        } catch (Exception e) {
            System.err.println("Failed to start console server: " + e.getMessage());
            e.printStackTrace();
            stop();
        }
    }

    public void stop() {
        System.out.println("Shutting down console server...");

        try {
            // Сохраняем финальный снапшот
            if (snapshotService != null) {
                snapshotService.saveSnapshot();
                System.out.println("Final snapshot saved");
            }
        } catch (Exception e) {
            System.err.println("Error saving final snapshot: " + e.getMessage());
        }

        // Останавливаем консольный процессор
        if (consoleProcessor != null) {
            consoleProcessor.stop();
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

        System.out.println("Console server shutdown complete");
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

    public Database getDatabase() {
        return database;
    }
}