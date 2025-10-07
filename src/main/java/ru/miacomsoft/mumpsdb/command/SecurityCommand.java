package ru.miacomsoft.mumpsdb.command;

import ru.miacomsoft.mumpsdb.core.Database;
import ru.miacomsoft.mumpsdb.security.SecurityManager;
import ru.miacomsoft.mumpsdb.server.CommandParser;

import java.util.regex.Pattern;

/**
 * Команды для управления безопасностью
 */
public class SecurityCommand {

    public static final String[] ALIASES = {"LOGIN", "LOGOUT", "CREATEUSER"};
    public static final String DESCRIPTION = "LOGIN username password - Authenticate user\n" +
            "LOGOUT - Logout current user\n" +
            "CREATEUSER username password role - Create new user (admin only)";
    public static final Pattern PATTERN = Pattern.compile(
            "^(LOGIN|LOGOUT|CREATEUSER)(?:\\s+(.+))?$",
            Pattern.CASE_INSENSITIVE
    );

    private final Database database;

    public SecurityCommand(Database database) {
        this.database = database;
    }

    public String execute(CommandParser.Command command) {
        try {
            String operation = command.getType().name();
            String[] args = parseArguments((String) command.getValue());

            SecurityManager security = database.getSecurityManager();

            switch (operation.toUpperCase()) {
                case "LOGIN":
                    if (args.length < 2) {
                        return "ERROR: Usage: LOGIN username password";
                    }
                    ru.miacomsoft.mumpsdb.security.UserSession session =
                            security.authenticate(args[0], args[1]);
                    if (session != null) {
                        return "OK: Logged in as " + args[0] + ", session: " + session.getSessionId();
                    } else {
                        return "ERROR: Invalid credentials";
                    }

                case "LOGOUT":
                    // Здесь должна быть логика получения sessionId из контекста
                    return "OK: Logged out";

                case "CREATEUSER":
                    if (args.length < 3) {
                        return "ERROR: Usage: CREATEUSER username password role";
                    }
                    // Здесь должна быть проверка прав текущего пользователя
                    boolean created = security.createUser(null, args[0], args[1], args[2]);
                    return created ? "OK: User created" : "ERROR: Failed to create user";

                default:
                    return "ERROR: Unknown security operation: " + operation;
            }

        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private String[] parseArguments(String argsStr) {
        if (argsStr == null) return new String[0];
        return argsStr.split("\\s+");
    }
}