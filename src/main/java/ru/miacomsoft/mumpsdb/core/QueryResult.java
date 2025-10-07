package ru.miacomsoft.mumpsdb.core;

import java.io.Serializable;

/**
 * Результат запроса к дереву
 */
public class QueryResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Object[] path;
    private final Object value;

    public QueryResult(Object[] path, Object value) {
        this.path = path;
        this.value = value;
    }

    public Object[] getPath() {
        return path;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Object p : path) {
            if (p instanceof String) {
                sb.append("(\"").append(p).append("\")");
            } else {
                sb.append("(").append(p).append(")");
            }
        }
        if (value != null) {
            sb.append(" = ").append(value);
        }
        return sb.toString();
    }
}