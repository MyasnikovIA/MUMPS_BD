
package ru.miacomsoft.mumpsdb.security;

import ru.miacomsoft.mumpsdb.server.CommandParser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Менеджер безопасности для аутентификации и авторизации
 */
public class SecurityManager {
    private final Map<String, User> users = new ConcurrentHashMap<>();
    private final Map<String, Set<Permission>> rolePermissions = new ConcurrentHashMap<>();
    private final Map<String, UserSession> activeSessions = new ConcurrentHashMap<>();

    // Права доступа по умолчанию
    private final Set<Permission> defaultPermissions = EnumSet.of(
            Permission.READ, Permission.WRITE, Permission.SEARCH
    );

    public SecurityManager() {
        initializeDefaultRoles();
        createDefaultAdminUser();
    }

    private void initializeDefaultRoles() {
        // Роль администратора - полный доступ
        rolePermissions.put("admin", EnumSet.allOf(Permission.class));

        // Роль пользователя - базовый доступ
        rolePermissions.put("user", EnumSet.of(
                Permission.READ, Permission.WRITE, Permission.SEARCH,
                Permission.TRANSACTION
        ));

        // Роль гостя - только чтение
        rolePermissions.put("guest", EnumSet.of(Permission.READ, Permission.SEARCH));
    }

    private void createDefaultAdminUser() {
        User admin = new User("admin", "admin123", "Administrator", "admin");
        users.put("admin", admin);
    }

    /**
     * Аутентификация пользователя
     */
    public UserSession authenticate(String username, String password) {
        User user = users.get(username);
        if (user != null && user.verifyPassword(password)) {
            UserSession session = new UserSession(user);
            activeSessions.put(session.getSessionId(), session);
            return session;
        }
        return null;
    }

    /**
     * Проверка прав доступа для команды
     */
    public boolean checkPermission(User user, CommandParser.Command command) {
        if (user == null) return false;

        Set<Permission> permissions = rolePermissions.get(user.getRole());
        if (permissions == null) {
            permissions = defaultPermissions;
        }

        return hasPermissionForCommand(permissions, command);
    }

    /**
     * Проверка прав доступа для глобала
     */
    public boolean checkGlobalPermission(User user, String global, Permission permission) {
        if (user == null) return false;

        // Админы имеют доступ ко всем глобалам
        if ("admin".equals(user.getRole())) {
            return true;
        }

        Set<Permission> permissions = rolePermissions.get(user.getRole());
        return permissions != null && permissions.contains(permission);
    }

    private boolean hasPermissionForCommand(Set<Permission> permissions, CommandParser.Command command) {
        switch (command.getType()) {
            case SET:
            case KILL:
                return permissions.contains(Permission.WRITE);
            case GET:
            case QUERY:
            case ZWRITE:
                return permissions.contains(Permission.READ);
            case SIMILARITY_SEARCH:
            case EXACT_SEARCH:
            case FAST_SEARCH:
                return permissions.contains(Permission.SEARCH);
            case BEGIN_TRANSACTION:
            case COMMIT:
            case ROLLBACK:
                return permissions.contains(Permission.TRANSACTION);
            case STATS:
                return permissions.contains(Permission.ADMIN);
            default:
                return permissions.contains(Permission.READ);
        }
    }

    /**
     * Создание нового пользователя (только для админов)
     */
    public boolean createUser(User admin, String username, String password, String role) {
        if (!"admin".equals(admin.getRole())) {
            return false;
        }

        if (users.containsKey(username)) {
            return false;
        }

        User newUser = new User(username, password, username, role);
        users.put(username, newUser);
        return true;
    }

    /**
     * Получение сессии по ID
     */
    public UserSession getSession(String sessionId) {
        return activeSessions.get(sessionId);
    }

    /**
     * Выход из системы
     */
    public void logout(String sessionId) {
        activeSessions.remove(sessionId);
    }

    /**
     * Получение количества активных пользователей
     */
    public int getActiveUsersCount() {
        return activeSessions.size();
    }

    /**
     * Перечисление прав доступа
     */
    public enum Permission {
        READ,       // Чтение данных
        WRITE,      // Запись данных
        SEARCH,     // Поиск
        TRANSACTION,// Управление транзакциями
        ADMIN,      // Администрирование
        BACKUP,     // Резервное копирование
        REPLICATION // Управление репликацией
    }
}