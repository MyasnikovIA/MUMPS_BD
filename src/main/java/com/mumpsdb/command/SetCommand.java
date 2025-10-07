package com.mumpsdb.command;

import com.mumpsdb.core.Database;
import com.mumpsdb.server.CommandParser;

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

            System.out.println("DEBUG SET COMMAND: name=" + name + ", path=" + Arrays.toString(path) + ", value=" + value);

            // Обрабатываем функции в значении
            Object processedValue = value;
            if (value instanceof String) {
                String strValue = (String) value;
                System.out.println("DEBUG SET COMMAND: Processing value: " + strValue);
                processedValue = functionHandler.processFunctions(strValue);
                System.out.println("DEBUG SET COMMAND: Processed value: " + processedValue);
            }

            // Проверяем, является ли это локальной переменной (без ^)
            if (isLocalVariable(name)) {
                if (writeCommand != null) {
                    writeCommand.setLocalVariable(name, processedValue);
                    System.out.println("DEBUG SET COMMAND: Set local variable '" + name + "' = '" + processedValue + "'");
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
}