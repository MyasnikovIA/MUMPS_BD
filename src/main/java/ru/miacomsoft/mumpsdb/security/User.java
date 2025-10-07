
package ru.miacomsoft.mumpsdb.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Класс пользователя системы
 */
public class User {
    private final String username;
    private final String passwordHash;
    private final String displayName;
    private final String role;
    private final long createdAt;

    public User(String username, String password, String displayName, String role) {
        this.username = username;
        this.passwordHash = hashPassword(password);
        this.displayName = displayName;
        this.role = role;
        this.createdAt = System.currentTimeMillis();
    }

    private String hashPassword(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(password.getBytes());
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }

    public boolean verifyPassword(String password) {
        return passwordHash.equals(hashPassword(password));
    }

    // Getters
    public String getUsername() { return username; }
    public String getDisplayName() { return displayName; }
    public String getRole() { return role; }
    public long getCreatedAt() { return createdAt; }

    @Override
    public String toString() {
        return String.format("User{username='%s', role='%s', displayName='%s'}",
                username, role, displayName);
    }
}