package ru.miacomsoft.mumpsdb.util;

import ru.miacomsoft.mumpsdb.ConfigLoader;

public class DebugUtil {
    private static Boolean debugEnabled = null;

    public static boolean isDebugEnabled() {
        if (debugEnabled == null) {
            ConfigLoader configLoader = new ConfigLoader();
            debugEnabled = Boolean.parseBoolean(
                    configLoader.getProperties().getProperty("database.debugger", "false")
            );
        }
        return debugEnabled;
    }

    public static void debug(String message) {
        if (isDebugEnabled()) {
            System.out.println("DEBUG: " + message);
        }
    }

    public static void debug(String format, Object... args) {
        if (isDebugEnabled()) {
            System.out.printf("DEBUG: " + format + "%n", args);
        }
    }
}