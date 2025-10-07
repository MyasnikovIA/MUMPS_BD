package ru.miacomsoft.mumpsdb.command;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.util.DebugUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Улучшенный обработчик функций MUMPS ($ORDER, $DATA, etc.)
 */
public class MumpsFunctionHandler {

    private final Database database;
    private WriteCommand writeCommand;

    // Паттерны для функций
    private static final Pattern ORDER_PATTERN = Pattern.compile(
            "\\$ORDER\\s*\\(\\s*([^,]+)\\s*(?:,\\s*([^)]*)\\s*)?(?:,\\s*(-?1))?\\s*\\)",
            Pattern.CASE_INSENSITIVE
    );

    public MumpsFunctionHandler(Database database) {
        this.database = database;
    }

    public void setWriteCommand(WriteCommand writeCommand) {
        this.writeCommand = writeCommand;
    }

    /**
     * Обрабатывает строку, выполняя подстановки функций
     */
    public Object processFunctions(String expression) {
        if (expression == null) return expression;

        DebugUtil.debug("FUNCTION HANDLER: Processing expression: %s", expression);

        String result = expression;

        try {
            // Обрабатываем $ORDER
            Matcher orderMatcher = ORDER_PATTERN.matcher(result);
            while (orderMatcher.find()) {
                DebugUtil.debug("FUNCTION HANDLER: Found $ORDER: %s", orderMatcher.group());
                String orderResult = executeOrderFunction(orderMatcher);
                DebugUtil.debug("FUNCTION HANDLER: $ORDER result: '%s'", orderResult);
                result = result.substring(0, orderMatcher.start()) +
                        orderResult +
                        result.substring(orderMatcher.end());
                orderMatcher = ORDER_PATTERN.matcher(result);
            }

            DebugUtil.debug("FUNCTION HANDLER: Final result: %s", result);
            return result;

        } catch (Exception e) {
            DebugUtil.debug("FUNCTION HANDLER: Error processing functions: %s", e.getMessage());
            // В случае ошибки возвращаем оригинальное выражение
            return expression;
        }
    }

    /**
     * Выполняет функцию $ORDER
     */
    private String executeOrderFunction(Matcher matcher) {
        try {
            String global = matcher.group(1).trim();
            String subscriptStr = matcher.group(2);
            String directionStr = matcher.group(3);

            DebugUtil.debug("ORDER FUNCTION: global=%s, subscriptStr=%s, directionStr=%s",
                    global, subscriptStr, directionStr);

            // Валидация входных параметров
            validateOrderParameters(global, subscriptStr);

            // Парсим подписки
            Object[] path;
            if (subscriptStr != null && !subscriptStr.trim().isEmpty()) {
                path = parsePath(subscriptStr);
            } else {
                path = new Object[0];
            }

            DebugUtil.debug("ORDER FUNCTION: parsed path=%s", Arrays.toString(path));

            // Определяем направление
            int direction = 1;
            if (directionStr != null && directionStr.equals("-1")) {
                direction = -1;
            }

            String result;

            if (path.length == 0) {
                // Поиск следующего глобала
                result = getNextGlobalIndex(global, direction);
            } else {
                // Поиск следующей подписки
                result = getNextSubscriptIndexSimple(global, path, direction);
            }

            DebugUtil.debug("ORDER FUNCTION: result='%s'", result);
            return result != null ? result : "";

        } catch (Exception e) {
            DebugUtil.debug("Error executing $ORDER: %s", e.getMessage());
            // В случае ошибки возвращаем пустую строку
            return "";
        }
    }

    private void validateOrderParameters(String global, String subscriptStr) {
        if (global == null || global.trim().isEmpty()) {
            throw new IllegalArgumentException("Global name cannot be empty in $ORDER");
        }

        // Проверяем корректность глобала
        if (!global.startsWith("^") && !isValidVariableName(global)) {
            throw new IllegalArgumentException("Invalid global name: " + global);
        }

        // Проверяем подписки если они есть
        if (subscriptStr != null && !subscriptStr.trim().isEmpty()) {
            try {
                parsePath(subscriptStr);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid subscript format: " + subscriptStr);
            }
        }
    }

    /**
     * Проверяет, является ли строка допустимым именем переменной
     */
    private boolean isValidVariableName(String name) {
        if (name == null || name.trim().isEmpty()) {
            return false;
        }

        // Проверяем, что это допустимое имя переменной (только буквы, цифры, подчеркивание)
        // и не содержит специальных символов
        return name.matches("[a-zA-Z_][a-zA-Z0-9_]*");
    }

    /**
     * Упрощенная версия для отладки
     */
    private String getNextSubscriptIndexSimple(String global, Object[] path, int direction) {
        try {
            DebugUtil.debug("SIMPLE ORDER: global=%s, path=%s", global, Arrays.toString(path));

            String globalName = normalizeGlobalName(global);
            List<String> nodes = database.getGlobalNodesZW(globalName);

            // Извлекаем все подписки первого уровня
            List<String> firstLevelSubscripts = new ArrayList<>();
            for (String node : nodes) {
                Object[] nodePath = parseNodePath(node);
                if (nodePath.length == 1) { // Подписки первого уровня: ^Test(1), ^Test(2)
                    firstLevelSubscripts.add(nodePath[0].toString());
                }
            }

            DebugUtil.debug("SIMPLE ORDER: First level subscripts: %s", firstLevelSubscripts);

            if (firstLevelSubscripts.isEmpty()) {
                return "";
            }

            Collections.sort(firstLevelSubscripts, this::compareSubscripts);

            // Получаем текущее значение переменной node
            String currentValue = "";
            if (path.length > 0 && path[0] instanceof String) {
                String varName = (String) path[0];
                if (writeCommand != null && isLocalVariable(varName)) {
                    Object varValue = writeCommand.getLocalVariable(varName);
                    currentValue = varValue != null ? varValue.toString() : "";
                }
            }

            DebugUtil.debug("SIMPLE ORDER: Current value: '%s'", currentValue);

            // Если текущее значение пустое, возвращаем первую подпись
            if (currentValue.isEmpty()) {
                return firstLevelSubscripts.get(0);
            }

            // Находим следующую подпись
            int currentIndex = -1;
            for (int i = 0; i < firstLevelSubscripts.size(); i++) {
                if (firstLevelSubscripts.get(i).equals(currentValue)) {
                    currentIndex = i;
                    break;
                }
            }

            if (currentIndex == -1) {
                return firstLevelSubscripts.get(0);
            }

            int nextIndex = currentIndex + direction;
            if (nextIndex < 0 || nextIndex >= firstLevelSubscripts.size()) {
                return "";
            }

            return firstLevelSubscripts.get(nextIndex);

        } catch (Exception e) {
            DebugUtil.debug("Error in simple order: %s", e.getMessage());
            return "";
        }
    }

    /**
     * Получает следующий/предыдущий глобал
     */
    private String getNextGlobalIndex(String currentGlobal, int direction) {
        List<String> allGlobals = database.getGlobalNames();
        if (allGlobals.isEmpty()) {
            return "";
        }

        Collections.sort(allGlobals);

        String normalizedCurrent = normalizeGlobalName(currentGlobal);

        // Находим позицию текущего глобала
        int currentIndex = -1;
        for (int i = 0; i < allGlobals.size(); i++) {
            if (allGlobals.get(i).equals(normalizedCurrent)) {
                currentIndex = i;
                break;
            }
        }

        if (currentIndex == -1) {
            // Глобал не найден - возвращаем первый
            return allGlobals.get(0);
        }

        int nextIndex = currentIndex + direction;
        if (nextIndex < 0 || nextIndex >= allGlobals.size()) {
            return "";
        }

        return allGlobals.get(nextIndex).substring(1); // Убираем ^
    }

    /**
     * Получает следующую/предыдущую подпись
     */
    private String getNextSubscriptIndex(String global, Object[] path, int direction) {
        try {
            String globalName = normalizeGlobalName(global);

            DebugUtil.debug("ORDER: global=%s, path=%s, direction=%s", global, Arrays.toString(path), direction);

            // Получаем все узлы глобала
            List<String> nodes = database.getGlobalNodesZW(globalName);
            DebugUtil.debug("ORDER: Found %d nodes", nodes.size());

            if (nodes.isEmpty()) {
                return "";
            }

            // Обрабатываем путь - заменяем переменные их значениями
            Object[] processedPath = processPathVariables(path);
            DebugUtil.debug("ORDER: Processed path: %s", Arrays.toString(processedPath));

            // Извлекаем дочерние подписки для обработанного пути
            List<String> childSubscripts = extractChildSubscripts(nodes, processedPath);
            DebugUtil.debug("ORDER: Child subscripts: %s", childSubscripts);

            if (childSubscripts.isEmpty()) {
                return "";
            }

            // Сортируем
            Collections.sort(childSubscripts, this::compareSubscripts);
            DebugUtil.debug("ORDER: Sorted subscripts: %s", childSubscripts);

            // Получаем текущую подпись (последний элемент обработанного пути)
            String currentSubscript = "";
            if (processedPath.length > 0) {
                Object lastElement = processedPath[processedPath.length - 1];
                currentSubscript = lastElement != null ? lastElement.toString() : "";
            }

            DebugUtil.debug("ORDER: Current subscript: '%s'", currentSubscript);

            // Если текущая подпись пустая, возвращаем первый/последний
            if (currentSubscript.isEmpty()) {
                String result = direction == 1 ? childSubscripts.get(0) : childSubscripts.get(childSubscripts.size() - 1);
                DebugUtil.debug("ORDER: Empty current, returning: '%s'", result);
                return result;
            }

            // Находим позицию текущей подписи
            int currentIndex = -1;
            for (int i = 0; i < childSubscripts.size(); i++) {
                if (childSubscripts.get(i).equals(currentSubscript)) {
                    currentIndex = i;
                    break;
                }
            }

            DebugUtil.debug("ORDER: Current index: %d", currentIndex);

            if (currentIndex == -1) {
                // Подпись не найдена - возвращаем первую/последнюю
                String result = direction == 1 ? childSubscripts.get(0) : childSubscripts.get(childSubscripts.size() - 1);
                DebugUtil.debug("ORDER: Current not found, returning: '%s'", result);
                return result;
            }

            int nextIndex = currentIndex + direction;
            if (nextIndex < 0 || nextIndex >= childSubscripts.size()) {
                DebugUtil.debug("ORDER: End of list reached");
                return "";
            }

            String result = childSubscripts.get(nextIndex);
            DebugUtil.debug("ORDER: Next subscript: '%s'", result);
            return result;

        } catch (Exception e) {
            DebugUtil.debug("Error in getNextSubscriptIndex: %s", e.getMessage());
            return "";
        }
    }

    /**
     * Обрабатывает путь, заменяя переменные их значениями
     */
    private Object[] processPathVariables(Object[] path) {
        if (path.length == 0) return path;

        Object[] processed = new Object[path.length];
        for (int i = 0; i < path.length; i++) {
            Object element = path[i];
            if (element instanceof String) {
                String strElement = (String) element;
                // Проверяем, является ли это именем переменной
                if (writeCommand != null && isLocalVariable(strElement)) {
                    Object varValue = writeCommand.getLocalVariable(strElement);
                    processed[i] = varValue != null ? varValue : "";
                    DebugUtil.debug("PROCESS PATH: Variable '%s' -> '%s'", strElement, processed[i]);
                } else {
                    processed[i] = element;
                }
            } else {
                processed[i] = element;
            }
        }
        return processed;
    }

    /**
     * Извлекает дочерние подписки
     */
    private List<String> extractChildSubscripts(List<String> nodes, Object[] parentPath) {
        List<String> childSubscripts = new ArrayList<>();
        // Мы ищем узлы, которые находятся на один уровень глубже parentPath
        int targetLevel = parentPath.length + 1;

        DebugUtil.debug("EXTRACT: Looking for children at level %d for path %s", targetLevel, Arrays.toString(parentPath));

        for (String node : nodes) {
            try {
                Object[] nodePath = parseNodePath(node);
                DebugUtil.debug("EXTRACT: Node path: %s (length: %d)", Arrays.toString(nodePath), nodePath.length);

                // Проверяем, что узел находится на нужном уровне и начинается с parentPath
                if (nodePath.length == targetLevel && startsWith(nodePath, parentPath)) {
                    Object lastElement = nodePath[nodePath.length - 1];
                    childSubscripts.add(lastElement.toString());
                    DebugUtil.debug("EXTRACT: Added child: %s", lastElement);
                }
            } catch (Exception e) {
                DebugUtil.debug("EXTRACT: Error parsing node: %s", e.getMessage());
            }
        }

        DebugUtil.debug("EXTRACT: Total children found: %d", childSubscripts.size());
        return childSubscripts;
    }

    // Вспомогательные методы
    private Object[] parsePath(String pathStr) {
        if (pathStr == null || pathStr.trim().isEmpty()) {
            return new Object[0];
        }

        List<Object> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '"';

        for (int i = 0; i < pathStr.length(); i++) {
            char c = pathStr.charAt(i);

            if ((c == '"' || c == '\'') && !inQuotes) {
                inQuotes = true;
                quoteChar = c;
            } else if (c == quoteChar && inQuotes) {
                inQuotes = false;
            } else if (c == ',' && !inQuotes) {
                elements.add(parsePathElement(current.toString().trim()));
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            elements.add(parsePathElement(current.toString().trim()));
        }

        return elements.toArray();
    }

    private Object parsePathElement(String element) {
        if (element.startsWith("\"") && element.endsWith("\"")) {
            return element.substring(1, element.length() - 1);
        }

        try {
            return Long.parseLong(element);
        } catch (NumberFormatException e) {
            return element;
        }
    }

    private Object[] parseNodePath(String node) {
        try {
            int eqIndex = node.indexOf('=');
            String pathPart = eqIndex > 0 ? node.substring(0, eqIndex) : node;

            int parenStart = pathPart.indexOf('(');
            if (parenStart > 0) {
                String pathStr = pathPart.substring(parenStart + 1, pathPart.length() - 1);
                return parsePath(pathStr);
            }
        } catch (Exception e) {
            // ignore
        }
        return new Object[0];
    }

    private boolean startsWith(Object[] array, Object[] prefix) {
        if (prefix.length > array.length) {
            DebugUtil.debug("STARTSWITH: Prefix longer than array");
            return false;
        }

        for (int i = 0; i < prefix.length; i++) {
            String arrayElem = array[i] != null ? array[i].toString() : "";
            String prefixElem = prefix[i] != null ? prefix[i].toString() : "";

            DebugUtil.debug("STARTSWITH: Comparing [%d]: '%s' vs '%s'", i, arrayElem, prefixElem);

            if (!arrayElem.equals(prefixElem)) {
                DebugUtil.debug("STARTSWITH: Mismatch at index %d", i);
                return false;
            }
        }

        DebugUtil.debug("STARTSWITH: Match found");
        return true;
    }

    private int compareSubscripts(String sub1, String sub2) {
        try {
            Long num1 = Long.parseLong(sub1);
            Long num2 = Long.parseLong(sub2);
            return num1.compareTo(num2);
        } catch (NumberFormatException e) {
            return sub1.compareTo(sub2);
        }
    }

    private String normalizeGlobalName(String global) {
        return global.startsWith("^") ? global : "^" + global;
    }

    private boolean isLocalVariable(String name) {
        return name != null && !name.startsWith("^") && name.matches("[a-zA-Z_][a-zA-Z0-9_]*");
    }
}