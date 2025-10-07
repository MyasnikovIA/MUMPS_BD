package ru.miacomsoft.mumpsdb.server;

import ru.miacomsoft.mumpsdb.command.*;
import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.core.Transaction;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Менеджер для динамической загрузки и выполнения команд MUMPS
 */
public class MumpsCommandManager {

    private static final Map<String, CommandInstance> commandCache = new HashMap<>();
    private static final String COMMANDS_PACKAGE = "ru.miacomsoft.mumpsdb.command";
    private static final String COMMANDS_DIR = "target/classes/com/mumpsdb/command";
    private static Database database;

    // Добавляем поле для хранения текущей транзакции
    private Transaction currentTransaction;

    // Карта для хранения информации о найденных командах
    private final Map<String, CommandInfo> commandInfoMap = new HashMap<>();

    /**
     * Инициализация менеджера команд
     */
    public void initialize(Database db) {
        database = db;
        scanCommandClasses();
        // Регистрируем новую команду быстрого поиска
        registerFastSearchCommand();
    }

    /**
     * Регистрация команды быстрого поиска
     */
    private void registerFastSearchCommand() {
        CommandInfo fastSearchInfo = new CommandInfo(
                "FastSearchCommand",
                "ru.miacomsoft.mumpsdb.command.FastSearchCommand",
                new String[]{"FSEARCH", "FS"},
                "FSEARCH value - Fast search by value using indexes"
        );
        commandInfoMap.put("FASTSEARCHCOMMAND", fastSearchInfo);
        commandInfoMap.put("FSEARCH", fastSearchInfo);
        commandInfoMap.put("FS", fastSearchInfo);
    }

    /**
     * Сканирует пакет команд и находит все классы с ALIASES, DESCRIPTION, PATTERN и execute методом
     */
    private void scanCommandClasses() {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            String packagePath = COMMANDS_PACKAGE.replace('.', '/');
            Enumeration<URL> resources = classLoader.getResources(packagePath);

            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                if (resource.getProtocol().equals("file")) {
                    scanDirectory(new File(resource.getFile()), COMMANDS_PACKAGE);
                }
            }

            System.out.println("Found " + commandInfoMap.size() + " command classes: " +
                    commandInfoMap.keySet());

        } catch (Exception e) {
            System.err.println("Error scanning command classes: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Рекурсивно сканирует директорию на наличие классов команд
     */
    private void scanDirectory(File directory, String packageName) {
        if (!directory.exists()) return;

        File[] files = directory.listFiles();
        if (files == null) return;

        for (File file : files) {
            if (file.isDirectory()) {
                scanDirectory(file, packageName + "." + file.getName());
            } else if (file.getName().endsWith(".class")) {
                String className = file.getName().substring(0, file.getName().length() - 6);
                String fullClassName = packageName + "." + className;

                try {
                    Class<?> clazz = Class.forName(fullClassName);
                    if (isValidCommandClass(clazz)) {
                        String[] aliases = (String[]) clazz.getField("ALIASES").get(null);
                        String description = (String) clazz.getField("DESCRIPTION").get(null);

                        CommandInfo info = new CommandInfo(className, fullClassName, aliases, description);
                        commandInfoMap.put(className.toUpperCase(), info);

                        for (String alias : aliases) {
                            commandInfoMap.put(alias.toUpperCase().replace(" ", "_"), info);
                        }
                    }
                } catch (Exception e) {
                    // Игнорируем классы, которые не являются командами
                }
            }
        }
    }

    // В методе getClassName добавляем case для FAST_SEARCH:
    private String getClassName(CommandParser.Command.Type commandType) {
        String typeName = commandType.name();

        // Пробуем найти по полному имени типа
        CommandInfo info = commandInfoMap.get(typeName);
        if (info != null) {
            return info.className;
        }

        // Пробуем найти с суффиксом "COMMAND"
        info = commandInfoMap.get(typeName + "COMMAND");
        if (info != null) {
            return info.className;
        }

        // Fallback для базовых команд
        switch (commandType) {
            case SET: return "SetCommand";
            case GET: return "GetCommand";
            case KILL: return "KillCommand";
            case ZWRITE: return "ZWruteCommand";
            case QUERY: return "QueryCommand";
            case SIMILARITY_SEARCH: return "SimilaritySearchCommand";
            case EXACT_SEARCH: return "ExactSearchCommand";
            case FAST_SEARCH: return "FastSearchCommand"; // Добавляем эту строку
            default:
                String simpleName = typeName.toLowerCase();
                if (!simpleName.endsWith("command")) {
                    simpleName += "command";
                }
                simpleName = simpleName.substring(0, 1).toUpperCase() + simpleName.substring(1);
                return simpleName;
        }
    }


    /**
     * Выполнить MUMPS команду
     */
    // В методе executeCommand добавляем обработку FAST_SEARCH:
    public String executeCommand(CommandParser.Command command) {
        if (command == null) {
            return "ERROR: Empty command";
        }

        try {
            // Для системных команд обрабатываем напрямую
            switch (command.getType()) {
                case BEGIN_TRANSACTION:
                    return processBeginTransaction();
                case COMMIT:
                    return processCommit();
                case ROLLBACK:
                    return processRollback();
                case STATS:
                    return processStats();
                case EXIT:
                    return "BYE";
                case ERROR:
                    return "ERROR: " + command.getErrorMessage();
                case HELP:
                    return getHelpText();
            }

            CommandInstance cmdInstance = getCommandInstance(command.getType());
            if (cmdInstance == null) {
                return "ERROR: Command '" + command.getTypeString() + "' not found or invalid";
            }

            Method executeMethod = cmdInstance.clazz.getMethod("execute", CommandParser.Command.class);
            return (String) executeMethod.invoke(cmdInstance.instance, command);

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR: " + e.getMessage();
        }
    }



    // Добавляем метод getHelpText
    private String getHelpText() {
        StringBuilder sb = new StringBuilder();
        sb.append("Available commands:\n");

        List<String> descriptions = getCommandDescriptions();
        for (String desc : descriptions) {
            sb.append("  ").append(desc).append("\n");
        }

        return sb.toString().trim();
    }


    /**
     * Получить список описаний всех доступных команд
     */
    public List<String> getCommandDescriptions() {
        List<String> descriptions = new ArrayList<>();

        for (CommandInfo info : commandInfoMap.values()) {
            if (!descriptions.contains(info.description)) {
                descriptions.add(info.description);
            }
        }

        descriptions.sort(String::compareToIgnoreCase);

        // Добавляем системные команды
        descriptions.add("TSTART/BEGIN TRANSACTION - Start transaction");
        descriptions.add("TCOMMIT/COMMIT - Commit transaction");
        descriptions.add("TROLLBACK/ROLLBACK - Rollback transaction");
        descriptions.add("STATS/$S - Show statistics");
        descriptions.add("FSEARCH/FS value - Fast search by value using indexes");
        descriptions.add("EXIT - Exit the server");

        return descriptions;
    }

    /**
     * Получить количество закэшированных команд
     */
    public int getCacheSize() {
        return commandCache.size();
    }

    /**
     * Получить количество найденных команд при сканировании
     */
    public int getDiscoveredCommandCount() {
        return (int) commandInfoMap.values().stream()
                .map(info -> info.fullClassName)
                .distinct()
                .count();
    }

    /**
     * Очистить кэш команд
     */
    public void clearCache() {
        commandCache.clear();
    }

    private CommandInstance getCommandInstance(CommandParser.Command.Type commandType) {
        String typeString = commandType.name();
        if (commandCache.containsKey(typeString)) {
            return commandCache.get(typeString);
        }

        try {
            String className = getClassName(commandType);
            Class<?> commandClass = loadCommandClass(className);

            if (commandClass != null && isValidCommandClass(commandClass)) {
                Object instance = commandClass.getDeclaredConstructor(Database.class).newInstance(database);

                CommandInstance cmdInstance = new CommandInstance(commandClass, instance);
                commandCache.put(typeString, cmdInstance);

                if (commandType == CommandParser.Command.Type.SET && instance instanceof SetCommand) {
                    CommandInstance writeInstance = getCommandInstance(CommandParser.Command.Type.WRITE);
                    if (writeInstance != null && writeInstance.instance instanceof WriteCommand) {
                        ((SetCommand) instance).setWriteCommand((WriteCommand) writeInstance.instance);
                        System.out.println("DEBUG: Linked SetCommand with WriteCommand and FunctionHandler");
                    }
                }
                return cmdInstance;
            }
        } catch (Exception e) {
            System.err.println("Error loading command '" + commandType + "': " + e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    private Class<?> loadCommandClass(String className) {
        try {
            CommandInfo info = commandInfoMap.get(className.toUpperCase());
            if (info != null) {
                return Class.forName(info.fullClassName);
            }

            String fullClassName = COMMANDS_PACKAGE + "." + className;
            return Class.forName(fullClassName);
        } catch (ClassNotFoundException e) {
            return loadClassFromFileSystem(className);
        }
    }

    private Class<?> loadClassFromFileSystem(String className) {
        try {
            File classDir = new File(COMMANDS_DIR);
            if (!classDir.exists()) {
                return null;
            }

            URLClassLoader classLoader = new URLClassLoader(
                    new URL[]{classDir.toURI().toURL()},
                    MumpsCommandManager.class.getClassLoader()
            );

            CommandInfo info = commandInfoMap.get(className.toUpperCase());
            if (info != null) {
                return classLoader.loadClass(info.fullClassName);
            }

            String fullClassName = COMMANDS_PACKAGE + "." + className;
            return classLoader.loadClass(fullClassName);

        } catch (Exception e) {
            return null;
        }
    }

    private boolean isValidCommandClass(Class<?> clazz) {
        try {
            clazz.getDeclaredField("ALIASES");
            clazz.getDeclaredField("DESCRIPTION");
            clazz.getDeclaredField("PATTERN");
            clazz.getMethod("execute", CommandParser.Command.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Методы для обработки системных команд
    private String processBeginTransaction() {
        try {
            if (currentTransaction != null) {
                return "ERROR: Transaction already in progress";
            }
            currentTransaction = database.beginTransaction();
            return "TRANSACTION STARTED";
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private String processCommit() {
        try {
            if (currentTransaction == null) {
                return "ERROR: No transaction in progress";
            }
            database.commitTransaction(currentTransaction);
            currentTransaction = null;
            return "TRANSACTION COMMITTED";
        } catch (Exception e) {
            currentTransaction = null;
            return "ERROR: " + e.getMessage();
        }
    }

    private String processRollback() {
        try {
            if (currentTransaction == null) {
                return "ERROR: No transaction in progress";
            }
            database.rollbackTransaction();
            currentTransaction = null;
            return "TRANSACTION ROLLED BACK";
        } catch (Exception e) {
            currentTransaction = null;
            return "ERROR: " + e.getMessage();
        }
    }

    private String processStats() {
        try {
            Map<String, Object> stats = database.getStats();
            return formatStats(stats);
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private String formatStats(Map<String, Object> stats) {
        StringBuilder sb = new StringBuilder();
        sb.append("Database Statistics:\n");
        for (Map.Entry<String, Object> entry : stats.entrySet()) {
            sb.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString().trim();
    }

    /**
     * Внутренний класс для хранения информации о команде
     */
    private static class CommandInfo {
        final String className;
        final String fullClassName;
        final String[] aliases;
        final String description;

        CommandInfo(String className, String fullClassName, String[] aliases, String description) {
            this.className = className;
            this.fullClassName = fullClassName;
            this.aliases = aliases;
            this.description = description;
        }
    }

    private static class CommandInstance {
        Class<?> clazz;
        Object instance;

        CommandInstance(Class<?> clazz, Object instance) {
            this.clazz = clazz;
            this.instance = instance;
        }
    }
}