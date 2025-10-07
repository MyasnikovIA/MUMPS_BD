package ru.miacomsoft.mumpsdb.persistence;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AOFManager {
    private final String aofFile;
    private final BlockingQueue<String> commandQueue;
    private volatile boolean running;
    private Thread writerThread;

    public AOFManager(String aofFile) {
        this.aofFile = aofFile;
        this.commandQueue = new LinkedBlockingQueue<>();
        this.running = false;
    }

    public void start() throws IOException {
        running = true;
        writerThread = new Thread(this::writeLoop, "AOF-Writer");
        writerThread.start();

        // Создаем файл если не существует
        new File(aofFile).createNewFile();

        System.out.println("AOF Manager started");
    }

    public void stop() {
        running = false;
        if (writerThread != null) {
            writerThread.interrupt();
            try {
                writerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("AOF Manager stopped");
    }

    public void appendCommand(String command) {
        if (running) {
            commandQueue.offer(command);
        }
    }

    private void writeLoop() {
        try (FileWriter writer = new FileWriter(aofFile, true);
             BufferedWriter bw = new BufferedWriter(writer)) {

            while (running || !commandQueue.isEmpty()) {
                try {
                    String command = commandQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (command != null) {
                        bw.write(command);
                        bw.newLine();
                        bw.flush();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (IOException e) {
            System.err.println("AOF write error: " + e.getMessage());
        }
    }

    public void replayCommands(CommandReplayer replayer) throws IOException {
        try (FileReader reader = new FileReader(aofFile);
             BufferedReader br = new BufferedReader(reader)) {

            String line;
            while ((line = br.readLine()) != null) {
                replayer.replayCommand(line.trim());
            }
        }
    }

    public interface CommandReplayer {
        void replayCommand(String command);
    }
}