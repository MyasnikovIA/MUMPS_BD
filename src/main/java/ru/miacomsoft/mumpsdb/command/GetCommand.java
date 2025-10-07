package ru.miacomsoft.mumpsdb.command;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.server.CommandParser;

import java.util.regex.Pattern;

/**
 * Команда GET для получения значений глобалов
 */
public class GetCommand {

    public static final String[] ALIASES = {"GET", "G"};
    public static final String DESCRIPTION = "GET/G ^global - Get global value";
    public static final Pattern PATTERN = Pattern.compile(
            "^G(?:ET)?\\s+(\\^?[^\\s(]+)(?:\\(([^)]+)\\))?$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public GetCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
        try {
            String global = command.getGlobal();
            Object[] path = command.getPath();

            Object result = database.get(global, path);

            if (result == null) {
                return "NULL";
            } else {
                return formatValue(result);
            }

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
}