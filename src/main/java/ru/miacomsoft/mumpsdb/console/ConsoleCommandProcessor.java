package ru.miacomsoft.mumpsdb.console;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.server.CommandParser;
import ru.miacomsoft.mumpsdb.server.MumpsCommandManager;

import java.util.Scanner;

/**
 * Обработчик команд для консольного режима
 */
public class ConsoleCommandProcessor {
    private final Database database;
    private final MumpsCommandManager commandManager;
    private boolean running;

    public ConsoleCommandProcessor(Database database) {
        this.database = database;
        this.commandManager = new MumpsCommandManager();
        this.commandManager.initialize(database);
        this.running = true;
    }

    public void start() {
        System.out.println("MUMPS-like Database Console Mode");
        System.out.println("Type 'HELP' for available commands, 'EXIT' to quit");

        Scanner scanner = new Scanner(System.in);

        while (running) {
            System.out.print("MUMPS> ");

            if (!scanner.hasNextLine()) {
                break;
            }

            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                continue;
            }

            if ("EXIT".equalsIgnoreCase(input) || "QUIT".equalsIgnoreCase(input)) {
                System.out.println("Goodbye!");
                break;
            }

            if ("HELP".equalsIgnoreCase(input)) {
                printHelp();
                continue;
            }

            // Обрабатываем команду
            processCommand(input);
        }

        scanner.close();
    }

    private void processCommand(String input) {
        try {
            CommandParser.Command command = CommandParser.parse(input);
            String response = commandManager.executeCommand(command);
            System.out.println(response);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  SET/S ^global=value                    - Set global value");
        System.out.println("  SET/S ^global(subscript)=value         - Set subscript value");
        System.out.println("  GET/G ^global                          - Get global value");
        System.out.println("  GET/G ^global(subscript)               - Get subscript value");
        System.out.println("  KILL/K ^global                         - Delete global");
        System.out.println("  KILL/K ^global(subscript)              - Delete subscript");
        System.out.println("  QUERY ^global DEPTH n                  - Query with depth");
        System.out.println("  LIST GLOBALS/LG [pattern]              - List all globals");
        System.out.println("  WRITE/W expression                     - Write data to output");
        System.out.println("  SIMSEARCH text [IN global] [TOP n]     - Semantic similarity search");
        System.out.println("  EXACTSEARCH text [IN global]           - Exact text search");
        System.out.println("  FSEARCH/FS value                       - Fast search by value");
        System.out.println("  TSTART/BEGIN TRANSACTION               - Start transaction");
        System.out.println("  TCOMMIT/COMMIT                         - Commit transaction");
        System.out.println("  TROLLBACK/ROLLBACK                     - Rollback transaction");
        System.out.println("  STATS/$S                               - Show statistics");
        System.out.println("  HELP                                   - Show this help");
        System.out.println("  EXIT/QUIT                              - Exit console mode");
        System.out.println();
    }

    public void stop() {
        running = false;
    }
}