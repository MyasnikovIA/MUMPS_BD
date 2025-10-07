package com.mumpsdb.command;

import com.mumpsdb.core.Database;
import com.mumpsdb.server.CommandParser;

import java.util.regex.Pattern;

/**
 * Команда KILL для удаления глобалов
 */
public class KillCommand {

    public static final String[] ALIASES = {"KILL", "K"};
    public static final String DESCRIPTION = "KILL/K ^global - Delete global";
    public static final Pattern PATTERN = Pattern.compile(
            "^K(?:ILL)?\\s+(\\^?[^\\s(]+)(?:\\(([^)]+)\\))?$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public KillCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
        try {
            String global = command.getGlobal();
            Object[] path = command.getPath();

            database.kill(global, path);
            return "OK";

        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }
}