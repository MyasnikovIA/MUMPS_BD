
package ru.miacomsoft.mumpsdb.backup;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.core.TreeNode;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Система инкрементального резервного копирования
 */
public class IncrementalBackup {
    private final Database database;
    private final String backupDir;
    private final Map<String, Long> lastBackupTimestamps = new HashMap<>();
    private long backupSequence = 0;

    public IncrementalBackup(Database database, String backupDir) {
        this.database = database;
        this.backupDir = backupDir;
        ensureBackupDirectory();
    }

    private void ensureBackupDirectory() {
        try {
            Files.createDirectories(Paths.get(backupDir));
            Files.createDirectories(Paths.get(backupDir, "full"));
            Files.createDirectories(Paths.get(backupDir, "incremental"));
            Files.createDirectories(Paths.get(backupDir, "logs"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create backup directory: " + backupDir, e);
        }
    }

    /**
     * Создание полного бэкапа
     */
    public String createFullBackup() throws IOException {
        String backupId = "full_" + System.currentTimeMillis();
        String backupFile = Paths.get(backupDir, "full", backupId + ".snapshot").toString();

        try (ObjectOutputStream oos = new ObjectOutputStream(
                new GZIPOutputStream(new FileOutputStream(backupFile)))) {

            Map<String, BackupMetadata> metadata = createBackupMetadata();
            oos.writeObject(metadata);
            oos.writeObject(database.getGlobalStorage());

            // Сохраняем метаданные бэкапа
            saveBackupMetadata(backupId, backupFile, metadata, true);
        }

        System.out.println("Full backup created: " + backupFile);
        return backupId;
    }

    /**
     * Создание инкрементального бэкапа
     */
    public String createIncrementalSnapshot() throws IOException {
        String backupId = "inc_" + (++backupSequence) + "_" + System.currentTimeMillis();
        String backupFile = Paths.get(backupDir, "incremental", backupId + ".snapshot").toString();

        Map<String, TreeNode> currentStorage = database.getGlobalStorage();
        Map<String, BackupMetadata> currentMetadata = createBackupMetadata();
        Map<String, ChangeSet> changes = new HashMap<>();

        // Определяем изменения с последнего бэкапа
        for (Map.Entry<String, BackupMetadata> entry : currentMetadata.entrySet()) {
            String global = entry.getKey();
            BackupMetadata meta = entry.getValue();
            Long lastBackup = lastBackupTimestamps.get(global);

            if (lastBackup == null || meta.getLastModified() > lastBackup) {
                // Глобал изменился, добавляем в changeset
                changes.put(global, new ChangeSet(global, currentStorage.get(global), meta));
            }
        }

        try (ObjectOutputStream oos = new ObjectOutputStream(
                new GZIPOutputStream(new FileOutputStream(backupFile)))) {

            oos.writeObject(changes);
            saveBackupMetadata(backupId, backupFile, currentMetadata, false);
        }

        // Обновляем временные метки
        updateBackupTimestamps(currentMetadata);

        System.out.println("Incremental backup created: " + backupFile + " (" + changes.size() + " changes)");
        return backupId;
    }

    /**
     * Восстановление из бэкапа
     */
    public void restoreFromBackup(String backupId) throws IOException {
        String backupFile = findBackupFile(backupId);
        if (backupFile == null) {
            throw new FileNotFoundException("Backup not found: " + backupId);
        }

        if (backupId.startsWith("full_")) {
            restoreFullBackup(backupFile);
        } else {
            restoreIncrementalBackup(backupFile);
        }

        System.out.println("Restored from backup: " + backupId);
    }

    /**
     * Восстановление на определенную дату
     */
    public void restoreToPointInTime(long timestamp) throws IOException {
        List<BackupInfo> backups = getAvailableBackups();
        List<BackupInfo> relevantBackups = new ArrayList<>();

        // Находим последний полный бэкап до указанной даты
        BackupInfo lastFullBackup = null;
        for (BackupInfo backup : backups) {
            if (backup.getTimestamp() <= timestamp) {
                if (backup.isFull()) {
                    lastFullBackup = backup;
                }
                relevantBackups.add(backup);
            }
        }

        if (lastFullBackup == null) {
            throw new IllegalArgumentException("No full backup found before timestamp: " + timestamp);
        }

        // Восстанавливаем полный бэкап
        restoreFullBackup(lastFullBackup.getFilePath());

        // Применяем инкрементальные бэкапы
        for (BackupInfo backup : relevantBackups) {
            if (!backup.isFull() && backup.getTimestamp() > lastFullBackup.getTimestamp()) {
                restoreIncrementalBackup(backup.getFilePath());
            }
        }

        System.out.println("Restored to point in time: " + new Date(timestamp));
    }

    @SuppressWarnings("unchecked")
    private void restoreFullBackup(String backupFile) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new java.util.zip.GZIPInputStream(new FileInputStream(backupFile)))) {

            Map<String, BackupMetadata> metadata = (Map<String, BackupMetadata>) ois.readObject();
            Map<String, TreeNode> storage = (Map<String, TreeNode>) ois.readObject();

            database.setGlobalStorage(storage);
            updateBackupTimestamps(metadata);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to restore backup: invalid format", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void restoreIncrementalBackup(String backupFile) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new java.util.zip.GZIPInputStream(new FileInputStream(backupFile)))) {

            Map<String, ChangeSet> changes = (Map<String, ChangeSet>) ois.readObject();

            // Применяем изменения к текущему состоянию
            Map<String, TreeNode> currentStorage = database.getGlobalStorage();
            for (ChangeSet change : changes.values()) {
                if (change.getTreeNode() == null) {
                    // Удаление
                    currentStorage.remove(change.getGlobal());
                } else {
                    // Добавление/изменение
                    currentStorage.put(change.getGlobal(), change.getTreeNode());
                }
            }

            database.setGlobalStorage(currentStorage);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to restore backup: invalid format", e);
        }
    }

    private Map<String, BackupMetadata> createBackupMetadata() {
        Map<String, TreeNode> storage = database.getGlobalStorage();
        Map<String, BackupMetadata> metadata = new HashMap<>();

        for (Map.Entry<String, TreeNode> entry : storage.entrySet()) {
            String global = entry.getKey();
            TreeNode tree = entry.getValue();

            BackupMetadata meta = new BackupMetadata(
                    global,
                    tree.countNodes(),
                    estimateMemoryUsage(tree),
                    System.currentTimeMillis()
            );
            metadata.put(global, meta);
        }

        return metadata;
    }

    private void saveBackupMetadata(String backupId, String backupFile,
                                    Map<String, BackupMetadata> metadata, boolean isFull) throws IOException {
        BackupInfo backupInfo = new BackupInfo(backupId, backupFile, System.currentTimeMillis(), isFull, metadata);
        String metaFile = Paths.get(backupDir, "logs", backupId + ".meta").toString();

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metaFile))) {
            oos.writeObject(backupInfo);
        }
    }

    private void updateBackupTimestamps(Map<String, BackupMetadata> metadata) {
        for (Map.Entry<String, BackupMetadata> entry : metadata.entrySet()) {
            lastBackupTimestamps.put(entry.getKey(), entry.getValue().getLastModified());
        }
    }

    private String findBackupFile(String backupId) {
        // Поиск в полных бэкапах
        Path fullPath = Paths.get(backupDir, "full", backupId + ".snapshot");
        if (Files.exists(fullPath)) {
            return fullPath.toString();
        }

        // Поиск в инкрементальных бэкапах
        Path incPath = Paths.get(backupDir, "incremental", backupId + ".snapshot");
        if (Files.exists(incPath)) {
            return incPath.toString();
        }

        return null;
    }

    public List<BackupInfo> getAvailableBackups() throws IOException {
        List<BackupInfo> backups = new ArrayList<>();

        // Читаем метаданные из логов
        Path logDir = Paths.get(backupDir, "logs");
        if (Files.exists(logDir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDir, "*.meta")) {
                for (Path metaFile : stream) {
                    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(metaFile.toFile()))) {
                        BackupInfo backupInfo = (BackupInfo) ois.readObject();
                        backups.add(backupInfo);
                    } catch (ClassNotFoundException e) {
                        System.err.println("Invalid backup metadata file: " + metaFile);
                    }
                }
            }
        }

        backups.sort(Comparator.comparingLong(BackupInfo::getTimestamp));
        return backups;
    }

    private long estimateMemoryUsage(TreeNode tree) {
        return tree.countNodes() * 100L; // Примерная оценка
    }

    /**
     * Очистка старых бэкапов
     */
    public void cleanupOldBackups(int keepDays) throws IOException {
        long cutoffTime = System.currentTimeMillis() - (keepDays * 24 * 60 * 60 * 1000L);
        List<BackupInfo> backups = getAvailableBackups();
        int deleted = 0;

        for (BackupInfo backup : backups) {
            if (backup.getTimestamp() < cutoffTime && !backup.isFull()) {
                // Удаляем инкрементальные бэкапы старше keepDays
                Files.deleteIfExists(Paths.get(backup.getFilePath()));
                Files.deleteIfExists(Paths.get(backupDir, "logs", backup.getBackupId() + ".meta"));
                deleted++;
            }
        }

        System.out.println("Cleaned up " + deleted + " old backups");
    }
}

/**
 * Метаданные бэкапа
 */
class BackupMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String global;
    private final int nodeCount;
    private final long estimatedSize;
    private final long lastModified;

    public BackupMetadata(String global, int nodeCount, long estimatedSize, long lastModified) {
        this.global = global;
        this.nodeCount = nodeCount;
        this.estimatedSize = estimatedSize;
        this.lastModified = lastModified;
    }

    // Getters
    public String getGlobal() {
        return global;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }

    public long getLastModified() {
        return lastModified;
    }
}

/**
 * Информация о бэкапе
 */
class BackupInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String backupId;
    private final String filePath;
    private final long timestamp;
    private final boolean full;
    private final Map<String, BackupMetadata> metadata;

    public BackupInfo(String backupId, String filePath, long timestamp,
                      boolean full, Map<String, BackupMetadata> metadata) {
        this.backupId = backupId;
        this.filePath = filePath;
        this.timestamp = timestamp;
        this.full = full;
        this.metadata = new HashMap<>(metadata);
    }

    // Getters
    public String getBackupId() {
        return backupId;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isFull() {
        return full;
    }

    public Map<String, BackupMetadata> getMetadata() {
        return new HashMap<>(metadata);
    }

    @Override
    public String toString() {
        return String.format("BackupInfo{id=%s, timestamp=%s, full=%s, globals=%d}",
                backupId, new Date(timestamp), full, metadata.size());
    }
}

/**
 * Набор изменений для инкрементального бэкапа
 */
class ChangeSet implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String global;
    private final TreeNode treeNode;
    private final BackupMetadata metadata;

    public ChangeSet(String global, TreeNode treeNode, BackupMetadata metadata) {
        this.global = global;
        this.treeNode = treeNode;
        this.metadata = metadata;
    }

    // Getters
    public String getGlobal() {
        return global;
    }

    public TreeNode getTreeNode() {
        return treeNode;
    }

    public BackupMetadata getMetadata() {
        return metadata;
    }
}