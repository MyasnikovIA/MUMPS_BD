package com.mumpsdb.command;

import com.mumpsdb.core.Database;
import com.mumpsdb.server.CommandParser;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Команда WRITE для вывода значений глобалов и текста
 */
public class WriteCommand {

    public static final String[] ALIASES = {"WRITE", "W"};
    public static final String DESCRIPTION = "WRITE/W expression - Write data to output";
    public static final Pattern PATTERN = Pattern.compile(
            "^W(?:RITE)?\\s+(.+)$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    // Хранилище для локальных переменных (в рамках сессии)
    private final Map<String, Object> localVariables = new HashMap<>();

    public WriteCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
        try {
            String expression = (String) command.getValue();

            if (expression == null || expression.trim().isEmpty()) {
                return ""; // Пустой вывод для WRITE без аргументов
            }

            // Обрабатываем выражение, которое может содержать:
            // 1. Текст в кавычках
            // 2. Ссылки на глобалы (^global)
            // 3. Локальные переменные (без ^)
            // 4. Комбинации текста и переменных
            return processExpression(expression.trim());

        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private String processExpression(String expression) {
        StringBuilder result = new StringBuilder();

        // Разбиваем выражение на части, разделенные запятыми
        String[] parts = splitExpression(expression);

        for (String part : parts) {
            String trimmedPart = part.trim();

            if (trimmedPart.isEmpty()) {
                continue;
            }

            // Проверяем, является ли часть строковым литералом
            if (isStringLiteral(trimmedPart)) {
                result.append(extractStringLiteral(trimmedPart));
            }
            // Проверяем, является ли часть ссылкой на глобал (начинается с ^)
            else if (isGlobalReference(trimmedPart)) {
                Object value = evaluateGlobalReference(trimmedPart);
                result.append(formatValue(value));
            }
            // Проверяем, является ли часть локальной переменной
            else if (isValidVariableName(trimmedPart)) {
                Object value = evaluateLocalVariable(trimmedPart);
                result.append(formatValue(value));
            }
            // Иначе считаем это строкой (для простоты)
            else {
                result.append(trimmedPart);
            }
        }

        return result.toString();
    }

    private String[] splitExpression(String expression) {
        // Разделяем по запятым, но учитываем строки в кавычках
        java.util.List<String> parts = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '"';

        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);

            if (c == '"' || c == '\'') {
                if (!inQuotes) {
                    inQuotes = true;
                    quoteChar = c;
                } else if (c == quoteChar) {
                    inQuotes = false;
                }
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                parts.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        // Добавляем последнюю часть
        if (current.length() > 0) {
            parts.add(current.toString().trim());
        }

        return parts.toArray(new String[0]);
    }

    private boolean isStringLiteral(String part) {
        return (part.startsWith("\"") && part.endsWith("\"")) ||
                (part.startsWith("'") && part.endsWith("'"));
    }

    private String extractStringLiteral(String part) {
        return part.substring(1, part.length() - 1);
    }

    private boolean isGlobalReference(String part) {
        return part.startsWith("^");
    }

    private boolean isValidVariableName(String part) {
        if (part == null || part.isEmpty()) {
            return false;
        }

        // Проверяем, что это допустимое имя переменной (только буквы, цифры, подчеркивание)
        // и не содержит специальных символов
        return part.matches("[a-zA-Z_][a-zA-Z0-9_]*");
    }

    private Object evaluateGlobalReference(String globalRef) {
        try {
            // Обрабатываем простые ссылки на глобалы вида ^global или ^global(subscript)
            if (globalRef.contains("(") && globalRef.endsWith(")")) {
                int parenStart = globalRef.indexOf('(');
                String globalName = globalRef.substring(0, parenStart);
                String subscriptStr = globalRef.substring(parenStart + 1, globalRef.length() - 1);

                Object[] path = parseSubscripts(subscriptStr);
                return database.get(globalName, path);
            } else {
                // Простой глобал без подстрочных индексов
                return database.get(globalRef);
            }
        } catch (Exception e) {
            System.err.println("Error evaluating global reference: " + globalRef + " - " + e.getMessage());
            return null;
        }
    }

    private Object evaluateLocalVariable(String variableName) {
        // Сначала проверяем локальные переменные
        if (localVariables.containsKey(variableName)) {
            return localVariables.get(variableName);
        }

        // Если локальной переменной нет, проверяем глобалы без ^
        // Это имитирует поведение MUMPS, где переменные могут быть локальными или глобальными
        try {
            String globalName = "^" + variableName;
            Object value = database.get(globalName);
            if (value != null) {
                return value;
            }
        } catch (Exception e) {
            // Игнорируем ошибки при доступе к глобалу
        }

        return null;
    }

    // Метод для установки локальных переменных (может быть вызван из других команд)
    public void setLocalVariable(String name, Object value) {
        if (isValidVariableName(name)) {
            localVariables.put(name, value);
            System.out.println("DEBUG: Set local variable '" + name + "' = '" + value + "'");
        }
    }

    // Метод для получения всех локальных переменных
    public Map<String, Object> getLocalVariables() {
        return new HashMap<>(localVariables);
    }


    // Метод для очистки локальных переменных
    public void clearLocalVariables() {
        localVariables.clear();
    }

    private Object[] parseSubscripts(String subscriptStr) {
        if (subscriptStr == null || subscriptStr.trim().isEmpty()) {
            return new Object[0];
        }

        java.util.List<Object> subscripts = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '"';

        for (int i = 0; i < subscriptStr.length(); i++) {
            char c = subscriptStr.charAt(i);

            if (c == '"' || c == '\'') {
                if (!inQuotes) {
                    inQuotes = true;
                    quoteChar = c;
                } else if (c == quoteChar) {
                    inQuotes = false;
                }
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                subscripts.add(parseValue(current.toString().trim()));
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        // Добавляем последний элемент
        if (current.length() > 0) {
            subscripts.add(parseValue(current.toString().trim()));
        }

        return subscripts.toArray();
    }

    private Object parseValue(String valueStr) {
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

    private String formatValue(Object value) {
        if (value == null) {
            return "";
        } else if (value instanceof String) {
            return (String) value;
        } else {
            return value.toString();
        }
    }

    public Object getLocalVariable(String name) {
        return localVariables.get(name);
    }

}