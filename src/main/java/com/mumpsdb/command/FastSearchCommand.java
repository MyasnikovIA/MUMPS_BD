package com.mumpsdb.command;

import com.mumpsdb.core.Database;
import com.mumpsdb.server.CommandParser;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Команда для быстрого поиска по значению с использованием индексов
 */
public class FastSearchCommand {

    public static final String[] ALIASES = {"FSEARCH", "FS"};
    public static final String DESCRIPTION = "FSEARCH value - Fast search by value using indexes";
    public static final Pattern PATTERN = Pattern.compile(
            "^F(?:SEARCH)?\\s+(.+)$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public FastSearchCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
        try {
            String searchValue = (String) command.getValue();

            if (searchValue == null || searchValue.trim().isEmpty()) {
                return "ERROR: Search value cannot be empty";
            }

            List<Database.SearchResult> results = database.fastSearch(searchValue.trim());

            if (results.isEmpty()) {
                return "NO RESULTS FOUND FOR: '" + searchValue + "'";
            }

            StringBuilder sb = new StringBuilder();
            sb.append("FAST SEARCH RESULTS:\n");

            for (int i = 0; i < results.size(); i++) {
                Database.SearchResult result = results.get(i);
                sb.append(String.format("%3d. %s%s = %s\n",
                        i + 1,
                        result.getGlobal(),
                        formatPath(result.getPath()),
                        formatValue(result.getValue())));
            }

            sb.append(String.format("\nTotal: %d result(s)", results.size()));
            return sb.toString();

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