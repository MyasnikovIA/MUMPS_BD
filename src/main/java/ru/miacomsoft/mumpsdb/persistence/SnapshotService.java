package ru.miacomsoft.mumpsdb.persistence;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.core.TreeNode;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SnapshotService {
    private final Database database;
    private final String snapshotFile;

    public SnapshotService(Database database, String snapshotFile) {
        this.database = database;
        this.snapshotFile = snapshotFile;
    }

    public void saveSnapshot() throws IOException {
        Map<String, TreeNode> storage = database.getGlobalStorage();

        try (FileOutputStream fos = new FileOutputStream(snapshotFile);
             GZIPOutputStream gzos = new GZIPOutputStream(fos);
             ObjectOutputStream oos = new ObjectOutputStream(gzos)) {

            oos.writeObject(storage);
            oos.flush();
        }
        SimpleDateFormat formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        String dateTime = formatter.format(new Date());
        System.out.println(dateTime+" Snapshot saved to: " + snapshotFile);
    }

    @SuppressWarnings("unchecked")
    public void loadSnapshot() throws IOException, ClassNotFoundException {
        File file = new File(snapshotFile);
        if (!file.exists()) {
            System.out.println("Snapshot file not found: " + snapshotFile);
            return;
        }

        try (FileInputStream fis = new FileInputStream(snapshotFile);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             ObjectInputStream ois = new ObjectInputStream(gzis)) {

            Map<String, TreeNode> storage = (Map<String, TreeNode>) ois.readObject();
            database.setGlobalStorage(storage);
        }

        System.out.println("Snapshot loaded from: " + snapshotFile);
    }
}