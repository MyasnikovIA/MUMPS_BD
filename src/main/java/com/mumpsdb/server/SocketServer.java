package com.mumpsdb.server;

import com.mumpsdb.core.Database;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SocketServer {
    private final int port;
    private final Database database;
    private final CommandProcessor processor;
    private final ExecutorService threadPool;
    private final List<ClientHandler> clients;
    private ServerSocket serverSocket;
    private boolean running;

    public SocketServer(int port, Database database) {
        this.port = port;
        this.database = database;
        this.processor = new CommandProcessor(database);
        this.threadPool = Executors.newCachedThreadPool();
        this.clients = new ArrayList<>();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        System.out.println("MUMPS-like Database Server started on port " + port);

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());

                ClientHandler clientHandler = new ClientHandler(clientSocket, processor);
                clients.add(clientHandler);
                threadPool.execute(clientHandler);

            } catch (IOException e) {
                if (running) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        }
    }

    public void stop() {
        running = false;

        // Останавливаем всех клиентов
        for (ClientHandler client : clients) {
            client.stopHandler();
        }

        // Завершаем thread pool
        threadPool.shutdown();

        // Закрываем server socket
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server socket: " + e.getMessage());
        }

        System.out.println("Server stopped");
    }
}