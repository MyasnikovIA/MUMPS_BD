package ru.miacomsoft.mumpsdb.command;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.server.CommandParser;
import ru.miacomsoft.mumpsdb.util.DebugUtil;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Команда SET для установки значений глобалов и локальных переменных
 */
public class SetCommand {

    public static final String[] ALIASES = {"SET", "S"};
    public static final String DESCRIPTION = "SET/S variable=value - Set variable or global value";
    public static final Pattern PATTERN = Pattern.compile(
            "^S(?:ET)?\\s+([a-zA-Z_\\^][^\\s(]*)(?:\\(([^)]+)\\))?\\s*=\\s*(.+)$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;
    private WriteCommand writeCommand;
    private MumpsFunctionHandler functionHandler;

    public SetCommand(Database database) {
        this.database = database;
        this.functionHandler = new MumpsFunctionHandler(database);
    }

    public void setWriteCommand(WriteCommand writeCommand) {
        this.writeCommand = writeCommand;
        this.functionHandler.setWriteCommand(writeCommand);
    }

    public String execute(CommandParser.Command command) {
        try {
            String name = command.getGlobal();
            Object[] path = command.getPath();
            Object value = command.getValue();

            DebugUtil.debug("SET COMMAND: name=%s, path=%s, value=%s",
                    name, Arrays.toString(path), value);

            // Валидация операции
            validateSetOperation(name, path, value);

            // Обрабатываем функции в значении
            Object processedValue = value;
            if (value instanceof String) {
                String strValue = (String) value;
                DebugUtil.debug("SET COMMAND: Processing value: %s", strValue);
                processedValue = functionHandler.processFunctions(strValue);
                DebugUtil.debug("SET COMMAND: Processed value: %s", processedValue);
            }

            // Проверяем, является ли это локальной переменной (без ^)
            if (isLocalVariable(name)) {
                if (writeCommand != null) {
                    writeCommand.setLocalVariable(name, processedValue);
                    DebugUtil.debug("SET COMMAND: Set local variable '%s' = '%s'", name, processedValue);
                    return "OK";
                } else {
                    return "ERROR: WriteCommand not initialized";
                }
            } else {
                // Это глобал
                database.set(name, processedValue, path);
                return "OK";
            }

        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private boolean isLocalVariable(String name) {
        return name != null && !name.startsWith("^") && name.matches("[a-zA-Z_][a-zA-Z0-9_]*");
    }

    // Валидация из пункта 9
    private void validateSetOperation(String global, Object[] path, Object value) {
        if (global == null || global.trim().isEmpty()) {
            throw new IllegalArgumentException("Global name cannot be empty");
        }
        if (!global.startsWith("^") && !global.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException("Invalid global name: " + global);
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        // Валидация пути
        if (path != null) {
            for (Object p : path) {
                if (p == null) {
                    throw new IllegalArgumentException("Path elements cannot be null");
                }
            }
        }
    }
}