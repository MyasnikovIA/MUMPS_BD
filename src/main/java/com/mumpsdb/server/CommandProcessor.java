package com.mumpsdb.server;

import com.mumpsdb.command.*;
import com.mumpsdb.core.Database;
import com.mumpsdb.core.Transaction;
import com.mumpsdb.embedding.VectorSearchResult;

import java.util.List;
import java.util.Map;

public class CommandProcessor {
    private final Database database;
    private Transaction currentTransaction;
    private final MumpsCommandManager commandManager;

    public CommandProcessor(Database database) {
        this.database = database;
        this.commandManager = new MumpsCommandManager();
        this.commandManager.initialize(database);
    }

    public String process(CommandParser.Command command) {
        // Обрабатываем HELP команду
        if (command.getType() == CommandParser.Command.Type.HELP) {
            return getHelpText();
        }

        return commandManager.executeCommand(command);
    }

    /**
     * Получить текст помощи
     */
    private String getHelpText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Available commands:\n");
        sb.append("  SET/S ^global=value                    - Set global value\n");
        sb.append("  SET/S ^global(subscript)=value         - Set subscript value\n");
        sb.append("  GET/G ^global                          - Get global value\n");
        sb.append("  GET/G ^global(subscript)               - Get subscript value\n");
        sb.append("  KILL/K ^global                         - Delete global\n");
        sb.append("  KILL/K ^global(subscript)              - Delete subscript\n");
        sb.append("  QUERY ^global DEPTH n                  - Query with depth\n");
        sb.append("  LIST GLOBALS/LG [pattern]              - List all globals (with optional filter)\n");
        sb.append("  WRITE/W expression                     - Write data to output\n");
        sb.append("  WRITE/W \"text\"                       - Write text\n");
        sb.append("  WRITE/W ^global                        - Write global value\n");
        sb.append("  WRITE/W variable                       - Write local variable\n");
        sb.append("  WRITE/W \"text\",^global,var           - Combine text, globals and variables\n");
        sb.append("  SIMSEARCH text [IN global] [TOP n]     - Semantic similarity search\n");
        sb.append("  EXACTSEARCH text [IN global]           - Exact text search\n");
        sb.append("  FSEARCH/FS value                       - Fast search by value using indexes\n");
        sb.append("  TSTART/BEGIN TRANSACTION               - Start transaction\n");
        sb.append("  TCOMMIT/COMMIT                         - Commit transaction\n");
        sb.append("  TROLLBACK/ROLLBACK                     - Rollback transaction\n");
        sb.append("  STATS/$S                               - Show statistics\n");
        sb.append("  HELP                                   - Show this help message\n");
        sb.append("Type 'EXIT' to quit");

        return sb.toString();
    }

    // Остальные методы без изменений...
    private String processSimilaritySearch(CommandParser.Command command) {
        try {
            String query = (String) command.getValue();
            String global = command.getGlobal();
            int topK = command.getDepth();

            List<VectorSearchResult> results;
            if (global != null) {
                results = database.similaritySearch(query, topK, global);
            } else {
                results = database.similaritySearch(query, topK);
            }

            if (results.isEmpty()) {
                return "NO SIMILAR RESULTS FOUND";
            }

            StringBuilder sb = new StringBuilder();
            sb.append("SIMILARITY SEARCH RESULTS:\n");
            for (VectorSearchResult result : results) {
                sb.append(String.format("Similarity: %.4f - %s%s = %s\n",
                        result.getSimilarity(),
                        result.getGlobal(),
                        formatPath(result.getPath()),
                        formatValue(result.getValue())));
            }
            return sb.toString().trim();
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private String processExactSearch(CommandParser.Command command) {
        try {
            String query = (String) command.getValue();
            String global = command.getGlobal();

            List<VectorSearchResult> results;
            if (global != null) {
                results = database.exactSearch(query, global);
            } else {
                results = database.exactSearch(query);
            }

            if (results.isEmpty()) {
                return "NO EXACT MATCHES FOUND";
            }

            StringBuilder sb = new StringBuilder();
            sb.append("EXACT SEARCH RESULTS:\n");
            for (VectorSearchResult result : results) {
                sb.append(String.format("%s%s = %s\n",
                        result.getGlobal(),
                        formatPath(result.getPath()),
                        formatValue(result.getValue())));
            }
            return sb.toString().trim();
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private String processBeginTransaction() {
        if (currentTransaction != null) {
            return "ERROR: Transaction already in progress";
        }

        currentTransaction = database.beginTransaction();
        return "TRANSACTION STARTED";
    }

    private String processCommit() {
        if (currentTransaction == null) {
            return "ERROR: No transaction in progress";
        }

        database.commitTransaction(currentTransaction);
        currentTransaction = null;
        return "TRANSACTION COMMITTED";
    }

    private String processRollback() {
        if (currentTransaction == null) {
            return "ERROR: No transaction in progress";
        }

        database.rollbackTransaction();
        currentTransaction = null;
        return "TRANSACTION ROLLED BACK";
    }

    private String processStats() {
        Map<String, Object> stats = database.getStats();
        return formatStats(stats);
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "\"" + value + "\"";
        } else {
            return value.toString();
        }
    }

    private String formatPath(Object[] path) {
        if (path.length == 0) return "";

        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < path.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(formatValue(path[i]));
        }
        sb.append(")");
        return sb.toString();
    }

    private String formatStats(Map<String, Object> stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("Database Statistics:\n");
        for (Map.Entry<String, Object> entry : stats.entrySet()) {
            sb.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString().trim();
    }
}