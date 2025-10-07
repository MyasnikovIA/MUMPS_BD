package com.mumpsdb.core;

import java.util.*;

public class Transaction {
    private final Database database;
    private final Map<String, TreeNode> transactionSnapshot;
    private final List<Operation> operations = new ArrayList<>();

    public Transaction(Database database) {
        this.database = database;
        this.transactionSnapshot = new HashMap<>();

        // Создаем глубокую копию данных для транзакции
        for (Map.Entry<String, TreeNode> entry : database.getGlobalStorage().entrySet()) {
            transactionSnapshot.put(entry.getKey(), entry.getValue().deepCopy());
        }
    }

    public void set(String global, Object... pathWithValue) {
        if (pathWithValue.length < 1) {
            throw new IllegalArgumentException("Must provide at least a value");
        }

        Object[] path;
        Object value;

        // Если передан только один аргумент - это значение глобала
        if (pathWithValue.length == 1) {
            path = new Object[0];
            value = pathWithValue[0];
        } else {
            // Иначе последний элемент - значение, остальные - путь
            path = Arrays.copyOf(pathWithValue, pathWithValue.length - 1);
            value = pathWithValue[pathWithValue.length - 1];
        }

        operations.add(new Operation(Operation.Type.SET, global, path, value));

        // Применяем к snapshot
        TreeNode tree = transactionSnapshot.computeIfAbsent(global, k -> new TreeNode());
        tree.setNode(path, value);
    }

    public void kill(String global, Object... path) {
        operations.add(new Operation(Operation.Type.KILL, global, path, null));

        // Применяем к snapshot
        if (path.length == 0) {
            transactionSnapshot.remove(global);
        } else {
            TreeNode tree = transactionSnapshot.get(global);
            if (tree != null) {
                tree.removeNode(path);
            }
        }
    }

    public Object get(String global, Object... path) {
        TreeNode tree = transactionSnapshot.get(global);
        return tree != null ? tree.getNode(path) : null;
    }

    public void commitTo(Database target) {
        target.setGlobalStorage(transactionSnapshot);
    }

    private static class Operation {
        enum Type { SET, KILL }

        final Type type;
        final String global;
        final Object[] path;
        final Object value;

        Operation(Type type, String global, Object[] path, Object value) {
            this.type = type;
            this.global = global;
            this.path = path;
            this.value = value;
        }
    }
}