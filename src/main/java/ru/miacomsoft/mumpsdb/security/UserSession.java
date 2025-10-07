
package ru.miacomsoft.mumpsdb.security;

import java.util.UUID;

/**
 * Сессия пользователя
 */
public class UserSession {
    private final String sessionId;
    private final User user;
    private final long createdAt;
    private long lastAccessTime;
    private boolean valid;

    public UserSession(User user) {
        this.sessionId = UUID.randomUUID().toString();
        this.user = user;
        this.createdAt = System.currentTimeMillis();
        this.lastAccessTime = this.createdAt;
        this.valid = true;
    }

    public void updateAccessTime() {
        this.lastAccessTime = System.currentTimeMillis();
    }

    public void invalidate() {
        this.valid = false;
    }

    public boolean isValid() {
        return valid && (System.currentTimeMillis() - lastAccessTime < 24 * 60 * 60 * 1000); // 24 часа
    }

    // Getters
    public String getSessionId() { return sessionId; }
    public User getUser() { return user; }
    public long getCreatedAt() { return createdAt; }
    public long getLastAccessTime() { return lastAccessTime; }
}