package com.mumpsdb.command;

import com.mumpsdb.core.Database;
import com.mumpsdb.server.CommandParser;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Команда для вывода списка всех глобалов в базе данных или просмотра узлов конкретного глобала
 */
public class ZWruteCommand {

    public static final String[] ALIASES = {"ZW", "ZWrute", "zw", "zwrite", "ZWRITE"};
    public static final String DESCRIPTION = "ZW [pattern|global_name] - List all globals with optional filter or view global nodes (ZWrute command)";
    public static final Pattern PATTERN = Pattern.compile(
            "^(?:ZW(?:rute)?|zw(?:rite)?)(?:\\s+(.+))?$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public ZWruteCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
        String argument = command.getFilter();

        if (argument != null && !argument.trim().isEmpty()) {
            String trimmedArg = argument.trim();

            // Проверяем, является ли аргумент именем глобала (начинается с ^)
            if (trimmedArg.startsWith("^")) {
                return executeGlobalView(trimmedArg);
            } else {
                return executeWithFilter(trimmedArg);
            }
        } else {
            return execute();
        }
    }

    /**
     * Выполняет команду без фильтра - список всех глобалов
     */
    private String execute() {
        List<String> globals = database.getGlobalNames();

        if (globals.isEmpty()) {
            return "NO GLOBALS";
        }

        StringBuilder result = new StringBuilder();
        result.append("GLOBALS LIST:\n");

        for (int i = 0; i < globals.size(); i++) {
            String global = globals.get(i);
            result.append(String.format("%3d. %s\n", i + 1, global));
        }

        result.append(String.format("\nTotal: %d global(s)", globals.size()));

        return result.toString();
    }

    /**
     * Выполняет команду с фильтром - поиск глобалов по шаблону
     */
    private String executeWithFilter(String pattern) {
        List<String> allGlobals = database.getGlobalNames();

        if (allGlobals.isEmpty()) {
            return "NO GLOBALS";
        }

        List<String> filteredGlobals;
        if (pattern != null && !pattern.trim().isEmpty()) {
            String filter = pattern.trim().toLowerCase();
            filteredGlobals = allGlobals.stream()
                    .filter(global -> global.toLowerCase().contains(filter))
                    .toList();
        } else {
            filteredGlobals = allGlobals;
        }

        if (filteredGlobals.isEmpty()) {
            return String.format("NO GLOBALS MATCHING PATTERN: '%s'", pattern);
        }

        StringBuilder result = new StringBuilder();
        if (pattern != null) {
            result.append(String.format("GLOBALS MATCHING '%s':\n", pattern));
        } else {
            result.append("GLOBALS LIST:\n");
        }

        for (int i = 0; i < filteredGlobals.size(); i++) {
            String global = filteredGlobals.get(i);
            result.append(String.format("%3d. %s\n", i + 1, global));
        }

        result.append(String.format("\nTotal: %d global(s)", filteredGlobals.size()));
        if (pattern != null) {
            result.append(String.format(" (filtered from %d)", allGlobals.size()));
        }

        return result.toString();
    }

    /**
     * Выполняет просмотр узлов конкретного глобала в формате MUMPS ZW
     */
    private String executeGlobalView(String globalName) {
        try {
            System.out.println("DEBUG: Getting nodes for global: " + globalName);

            // Получаем список узлов для указанного глобала в формате ZW
            List<String> nodes = database.getGlobalNodesZW(globalName);


            if (nodes.isEmpty()) {
                return String.format("NO NODES IN GLOBAL: %s", globalName);
            }

            StringBuilder result = new StringBuilder();
            for (String node : nodes) {
                result.append(node).append("\n");
            }

            return result.toString().trim();
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            return String.format("ERROR ACCESSING GLOBAL %s: %s", globalName, e.getMessage());
        }
    }
}

