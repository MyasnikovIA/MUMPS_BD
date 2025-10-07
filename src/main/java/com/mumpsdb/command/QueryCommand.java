package com.mumpsdb.command;

import com.mumpsdb.core.Database;
import com.mumpsdb.core.QueryResult;
import com.mumpsdb.server.CommandParser;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Команда QUERY для запросов к глобалам
 */
public class QueryCommand {

    public static final String[] ALIASES = {"QUERY", "Q"};
    public static final String DESCRIPTION = "QUERY ^global DEPTH n - Query with depth";
    public static final Pattern PATTERN = Pattern.compile(
            "^QUERY\\s+([^\\s(]+)(?:\\(([^)]+)\\))?(?:\\s+DEPTH\\s+(\\d+|-1))?$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public QueryCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
        try {
            String global = command.getGlobal();
            Object[] path = command.getPath();
            int depth = command.getDepth();

            List<QueryResult> results = database.query(global, path, depth);

            if (results.isEmpty()) {
                return "NO RESULTS";
            }

            StringBuilder sb = new StringBuilder();
            sb.append("QUERY RESULTS:\n");

            for (int i = 0; i < results.size(); i++) {
                QueryResult result = results.get(i);
                sb.append(String.format("%3d. ", i + 1))
                        .append(formatResult(result))
                        .append("\n");
            }

            sb.append(String.format("\nTotal: %d result(s)", results.size()));

            return sb.toString();

        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private String formatResult(QueryResult result) {
        StringBuilder sb = new StringBuilder();

        // Форматируем путь
        Object[] path = result.getPath();
        if (path.length > 0) {
            sb.append("Path: [");
            for (int i = 0; i < path.length; i++) {
                if (i > 0) sb.append(", ");
                if (path[i] instanceof String) {
                    sb.append("\"").append(path[i]).append("\"");
                } else {
                    sb.append(path[i]);
                }
            }
            sb.append("]");
        }

        // Форматируем значение
        Object value = result.getValue();
        if (value != null) {
            if (path.length > 0) sb.append(" - ");
            sb.append("Value: ").append(formatValue(value));
        }

        return sb.toString();
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
}