package com.mumpsdb.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientHandler extends Thread {
    private final Socket clientSocket;
    private final CommandProcessor processor;
    private final AtomicBoolean running;
    private BufferedReader in;
    private PrintWriter out;

    public ClientHandler(Socket socket, CommandProcessor processor) {
        this.clientSocket = socket;
        this.processor = processor;
        this.running = new AtomicBoolean(true);
    }

    @Override
    public void run() {
        try {
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            out = new PrintWriter(clientSocket.getOutputStream(), true);

            out.println("Welcome to MUMPS-like Database Server");
            out.println("Available commands:");

            // Получаем описания команд из MumpsCommandManager
            out.println("  SET/S ^global=value                    - Set global value");
            out.println("  SET/S ^global(subscript)=value         - Set subscript value");
            out.println("  GET/G ^global                          - Get global value");
            out.println("  GET/G ^global(subscript)               - Get subscript value");
            out.println("  KILL/K ^global                         - Delete global");
            out.println("  KILL/K ^global(subscript)              - Delete subscript");
            out.println("  QUERY ^global DEPTH n                  - Query with depth");
            out.println("  LIST GLOBALS/LG [pattern]              - List all globals (with optional filter)");
            out.println("  WRITE/W expression                     - Write data to output");
            out.println("  WRITE/W \"text\"                       - Write text");
            out.println("  WRITE/W ^global                        - Write global value");
            out.println("  WRITE/W variable                       - Write local variable");
            out.println("  WRITE/W \"text\",^global,var           - Combine text, globals and variables");
            out.println("  SIMSEARCH text [IN global] [TOP n]     - Semantic similarity search");
            out.println("  EXACTSEARCH text [IN global]           - Exact text search");
            out.println("  FSEARCH/FS value                       - Fast search by value using indexes");
            out.println("  TSTART/BEGIN TRANSACTION               - Start transaction");
            out.println("  TCOMMIT/COMMIT                         - Commit transaction");
            out.println("  TROLLBACK/ROLLBACK                     - Rollback transaction");
            out.println("  STATS/$S                               - Show statistics");
            out.println("  HELP                                   - Show this help message");
            out.println("Type 'EXIT' to quit");
            out.println("");

            String inputLine;
            while (running.get() && (inputLine = in.readLine()) != null) {
                String trimmed = inputLine.trim();
                if (trimmed.isEmpty()) {
                    out.print("> ");
                    out.flush();
                    continue;
                }

                if ("EXIT".equalsIgnoreCase(trimmed)) {
                    out.println("Goodbye!");
                    break;
                }

                if ("HELP".equalsIgnoreCase(trimmed)) {
                    printHelp();
                    out.print("> ");
                    out.flush();
                    continue;
                }

                CommandParser.Command command = CommandParser.parse(trimmed);
                String response = processor.process(command);
                out.println(response);

                out.print("> ");
                out.flush();
            }

        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        } finally {
            closeConnection();
        }
    }

    private void printHelp() {
        out.println("Available commands:");
        out.println("  SET/S ^global=value                    - Set global value");
        out.println("  SET/S ^global(subscript)=value         - Set subscript value");
        out.println("  GET/G ^global                          - Get global value");
        out.println("  GET/G ^global(subscript)               - Get subscript value");
        out.println("  KILL/K ^global                         - Delete global");
        out.println("  KILL/K ^global(subscript)              - Delete subscript");
        out.println("  QUERY ^global DEPTH n                  - Query with depth");
        out.println("  LIST GLOBALS/LG [pattern]              - List all globals");
        out.println("  WRITE/W expression                     - Write data to output");
        out.println("  SIMSEARCH text [IN global] [TOP n]     - Semantic similarity search");
        out.println("  EXACTSEARCH text [IN global]           - Exact text search");
        out.println("  FSEARCH/FS value                       - Fast search by value");
        out.println("  TSTART/BEGIN TRANSACTION               - Start transaction");
        out.println("  TCOMMIT/COMMIT                         - Commit transaction");
        out.println("  TROLLBACK/ROLLBACK                     - Rollback transaction");
        out.println("  STATS/$S                               - Show statistics");
        out.println("  HELP                                   - Show this help");
        out.println("  EXIT                                   - Exit the server");
    }

    public void stopHandler() {
        running.set(false);
        closeConnection();
    }

    private void closeConnection() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (clientSocket != null) clientSocket.close();
        } catch (IOException e) {
            System.err.println("Error closing client connection: " + e.getMessage());
        }
    }
}