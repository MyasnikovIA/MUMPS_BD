package ru.miacomsoft.mumpsdb.command;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.embedding.VectorSearchResult;
import ru.miacomsoft.mumpsdb.server.CommandParser;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Команда для точного поиска
 */
public class ExactSearchCommand {

    public static final String[] ALIASES = {"EXACTSEARCH"};
    public static final String DESCRIPTION = "EXACTSEARCH query [IN global] - Exact search";
    public static final Pattern PATTERN = Pattern.compile(
            "^EXACTSEARCH\\s+(.+?)(?:\\s+IN\\s+(\\^?[^\\s]+))?$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public ExactSearchCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
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
}