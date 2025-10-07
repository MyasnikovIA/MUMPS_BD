package ru.miacomsoft.mumpsdb.server;

import ru.miacomsoft.mumpsdb.command.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

public class CommandParser {

    public static Command parse(String input) {
        if (input == null || input.trim().isEmpty()) {
            return new Command(Command.Type.ERROR, "Empty command");
        }

        String trimmed = input.trim();

        // Дополнительная проверка на очень короткие или пустые команды после трима
        if (trimmed.length() == 0) {
            return new Command(Command.Type.ERROR, "Empty command");
        }

        // Короткие команды транзакций
        if (trimmed.equalsIgnoreCase("TSTART") || trimmed.equalsIgnoreCase("BEGIN TRANSACTION")) {
            return new Command(Command.Type.BEGIN_TRANSACTION);
        }

        if (trimmed.equalsIgnoreCase("TCOMMIT") || trimmed.equalsIgnoreCase("COMMIT")) {
            return new Command(Command.Type.COMMIT);
        }

        if (trimmed.equalsIgnoreCase("TROLLBACK") || trimmed.equalsIgnoreCase("ROLLBACK")) {
            return new Command(Command.Type.ROLLBACK);
        }

        // Короткие команды системных операций
        if (trimmed.equalsIgnoreCase("STATS") || trimmed.equalsIgnoreCase("$S")) {
            return new Command(Command.Type.STATS);
        }

        // Команда HELP
        if (trimmed.equalsIgnoreCase("HELP")) {
            return new Command(Command.Type.HELP);
        }

        // Парсим команды с использованием PATTERN из соответствующих классов
        Matcher zwriteMatcher = ZWruteCommand.PATTERN.matcher(trimmed);
        if (zwriteMatcher.matches()) {
            return parseZWruteCommand(zwriteMatcher);
        }

        Matcher writeMatcher = WriteCommand.PATTERN.matcher(trimmed);
        if (writeMatcher.matches()) {
            return parseWriteCommand(writeMatcher);
        }

        Matcher setMatcher = SetCommand.PATTERN.matcher(trimmed);
        if (setMatcher.matches()) {
            return parseSetCommand(setMatcher);
        }

        Matcher getMatcher = GetCommand.PATTERN.matcher(trimmed);
        if (getMatcher.matches()) {
            return parseGetCommand(getMatcher);
        }

        Matcher killMatcher = KillCommand.PATTERN.matcher(trimmed);
        if (killMatcher.matches()) {
            return parseKillCommand(killMatcher);
        }

        Matcher queryMatcher = QueryCommand.PATTERN.matcher(trimmed);
        if (queryMatcher.matches()) {
            return parseQueryCommand(queryMatcher);
        }

        Matcher simSearchMatcher = SimilaritySearchCommand.PATTERN.matcher(trimmed);
        if (simSearchMatcher.matches()) {
            return parseSimilaritySearchCommand(simSearchMatcher);
        }

        Matcher exactSearchMatcher = ExactSearchCommand.PATTERN.matcher(trimmed);
        if (exactSearchMatcher.matches()) {
            return parseExactSearchCommand(exactSearchMatcher);
        }

        // Добавляем парсинг для FSEARCH
        Matcher fastSearchMatcher = FastSearchCommand.PATTERN.matcher(trimmed);
        if (fastSearchMatcher.matches()) {
            return parseFastSearchCommand(fastSearchMatcher);
        }

        return new Command(Command.Type.ERROR, "Unknown command: " + trimmed);
    }

    // Добавляем метод парсинга для FSEARCH
    private static Command parseFastSearchCommand(Matcher matcher) {
        try {
            String query = matcher.group(1).trim();
            return Command.createFastSearchCommand(query);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid FSEARCH command: " + e.getMessage());
        }
    }

    // Остальные методы без изменений...
    private static Command parseZWruteCommand(Matcher matcher) {
        try {
            String filter = matcher.group(1); // Опциональный фильтр
            return Command.createZWruteCommand(filter);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid ZW command: " + e.getMessage());
        }
    }

    private static Command parseWriteCommand(Matcher matcher) {
        try {
            String expression = matcher.group(1).trim();
            return Command.createWriteCommand(expression);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid WRITE command: " + e.getMessage());
        }
    }

    private static Command parseSetCommand(Matcher matcher) {
        try {
            String global = matcher.group(1);
            String pathStr = matcher.group(2);
            String valueStr = matcher.group(3);

            Object[] path = pathStr != null ? parsePath(pathStr) : new Object[0];
            Object value = valueStr != null ? valueStr.trim() : null;

            return new Command(Command.Type.SET, global, path, value);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid SET command: " + e.getMessage());
        }
    }

    private static Command parseGetCommand(Matcher matcher) {
        try {
            String global = matcher.group(1);
            String pathStr = matcher.group(2);

            Object[] path = pathStr != null ? parsePath(pathStr) : new Object[0];

            return new Command(Command.Type.GET, global, path);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid GET command: " + e.getMessage());
        }
    }

    private static Command parseKillCommand(Matcher matcher) {
        try {
            String global = matcher.group(1);
            String pathStr = matcher.group(2);

            Object[] path = pathStr != null ? parsePath(pathStr) : new Object[0];

            return new Command(Command.Type.KILL, global, path);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid KILL command: " + e.getMessage());
        }
    }

    private static Command parseQueryCommand(Matcher matcher) {
        try {
            String global = matcher.group(1);
            String pathStr = matcher.group(2);
            String depthStr = matcher.group(3);

            Object[] path = pathStr != null ? parsePath(pathStr) : new Object[0];
            int depth = depthStr != null ? Integer.parseInt(depthStr) : 1;

            return new Command(Command.Type.QUERY, global, path, depth);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid QUERY command: " + e.getMessage());
        }
    }

    private static Command parseSimilaritySearchCommand(Matcher matcher) {
        try {
            String query = matcher.group(1).trim();
            String global = matcher.group(2);
            String topStr = matcher.group(3);

            int topK = topStr != null ? Integer.parseInt(topStr) : 10;

            return new Command(Command.Type.SIMILARITY_SEARCH, query, global, topK);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid SIMSEARCH command: " + e.getMessage());
        }
    }

    private static Command parseExactSearchCommand(Matcher matcher) {
        try {
            String query = matcher.group(1).trim();
            String global = matcher.group(2);

            return new Command(Command.Type.EXACT_SEARCH, query, global);
        } catch (Exception e) {
            return new Command(Command.Type.ERROR, "Invalid EXACTSEARCH command: " + e.getMessage());
        }
    }

    public static Object[] parsePath(String pathStr) {
        if (pathStr == null || pathStr.trim().isEmpty()) {
            return new Object[0];
        }

        // Разделяем по запятым, учитывая строки в кавычках
        List<Object> path = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '"';

        for (int i = 0; i < pathStr.length(); i++) {
            char c = pathStr.charAt(i);

            if (c == '"' || c == '\'') {
                if (!inQuotes) {
                    inQuotes = true;
                    quoteChar = c;
                } else if (c == quoteChar) {
                    inQuotes = false;
                } else {
                    current.append(c);
                }
            } else if (c == ',' && !inQuotes) {
                path.add(parseValue(current.toString().trim()));
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        // Добавляем последний элемент
        if (current.length() > 0) {
            path.add(parseValue(current.toString().trim()));
        }

        return path.toArray();
    }

    private static Object parseValue(String valueStr) {
        if (valueStr == null) return null;

        String trimmed = valueStr.trim();

        // Проверяем строку в кавычках
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
                (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }

        // Проверяем число
        try {
            if (trimmed.contains(".")) {
                return Double.parseDouble(trimmed);
            } else {
                return Long.parseLong(trimmed);
            }
        } catch (NumberFormatException e) {
            // Не число, возвращаем как строку
            return trimmed;
        }
    }

    public static class Command {
        public enum Type {
            SET, GET, KILL, QUERY, WRITE, BEGIN_TRANSACTION, COMMIT, ROLLBACK,
            STATS, ZWRITE, ERROR, EXIT, SIMILARITY_SEARCH, EXACT_SEARCH, HELP, FAST_SEARCH
        }

        private final Type type;
        private final String global;
        private final Object[] path;
        private final Object value;
        private final int depth;
        private final String errorMessage;
        private final String filter;

        // Базовые конструкторы
        public Command(Type type) {
            this(type, null, new Object[0], null, -1, null, null);
        }

        public Command(Type type, String errorMessage) {
            this(type, null, new Object[0], null, -1, errorMessage, null);
        }

        public Command(Type type, String global, Object[] path) {
            this(type, global, path, null, -1, null, null);
        }

        public Command(Type type, String global, Object[] path, Object value) {
            this(type, global, path, value, -1, null, null);
        }

        public Command(Type type, String global, Object[] path, int depth) {
            this(type, global, path, null, depth, null, null);
        }

        // Для поисковых команд
        public Command(Type type, String query, String global, int topK) {
            this(type, global, new Object[0], query, topK, null, null);
        }

        public Command(Type type, String query, String global) {
            this(type, global, new Object[0], query, -1, null, null);
        }

        // Главный приватный конструктор
        private Command(Type type, String global, Object[] path, Object value,
                        int depth, String errorMessage, String filter) {
            this.type = type;
            this.global = global;
            this.path = path != null ? path : new Object[0];
            this.value = value;
            this.depth = depth;
            this.errorMessage = errorMessage;
            this.filter = filter;
        }

        // Конструктор для WRITE команды
        public static Command createWriteCommand(String expression) {
            return new Command(Type.WRITE, null, new Object[0], expression, -1, null, null);
        }

        // Конструктор для ZW команды
        public static Command createZWruteCommand(String filter) {
            return new Command(Type.ZWRITE, null, new Object[0], null, -1, null, filter);
        }

        // Конструктор для FAST_SEARCH команды
        public static Command createFastSearchCommand(String query) {
            return new Command(Type.FAST_SEARCH, null, new Object[0], query, -1, null, null);
        }

        // Getters
        public Type getType() { return type; }
        public String getGlobal() { return global; }
        public Object[] getPath() { return path; }
        public Object getValue() { return value; }
        public int getDepth() { return depth; }
        public String getErrorMessage() { return errorMessage; }
        public String getFilter() { return filter; }
        public boolean isError() { return type == Type.ERROR; }

        // Новая функция для получения строкового представления типа команды
        public String getTypeString() {
            return type.name();
        }
    }
}