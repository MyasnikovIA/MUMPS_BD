package com.mumpsdb.command;

import com.mumpsdb.core.Database;
import com.mumpsdb.embedding.VectorSearchResult;
import com.mumpsdb.server.CommandParser;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Команда для поиска похожих значений
 */
public class SimilaritySearchCommand {

    public static final String[] ALIASES = {"SIMSEARCH"};
    public static final String DESCRIPTION = "SIMSEARCH query [IN global] [TOP n] - Similarity search";
    public static final Pattern PATTERN = Pattern.compile(
            "^SIMSEARCH\\s+(.+?)(?:\\s+IN\\s+(\\^?[^\\s]+))?(?:\\s+TOP\\s+(\\d+))?$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public SimilaritySearchCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
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