package ru.miacomsoft.mumpsdb;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
    private Properties properties;

    public ConfigLoader() {
        this("application.properties");
    }

    public ConfigLoader(String configFile) {
        properties = new Properties();
        try {
            // Сначала пробуем загрузить из файловой системы
            properties.load(new FileInputStream(configFile));
            System.out.println("Configuration loaded from: " + configFile);
        } catch (IOException e) {
            // Если не найден в файловой системе, пробуем из classpath
            properties = loadPropertiesFromClasspath(configFile);
            if (properties == null) {
                System.out.println("Configuration file not found, using default properties");
                properties = new Properties(); // Инициализируем properties
                setDefaultProperties();
            } else {
                System.out.println("Configuration loaded from classpath: " + configFile);
            }
        }
    }

    // Добавлен метод для проверки включения auto-embedding
    public boolean isAutoEmbeddingEnabled() {
        return Boolean.parseBoolean(properties.getProperty("database.auto.embedding.enabled", "true"));
    }

    // Остальные методы без изменений...
    private void setDefaultProperties() {
        // Server Configuration
        properties.setProperty("server.port", "9090");
        properties.setProperty("server.host", "localhost");
        properties.setProperty("server.max.connections", "100");
        properties.setProperty("server.connection.timeout", "30000");

        // Database Configuration
        properties.setProperty("database.auto.embedding.enabled", "true"); // Значение по умолчанию
        properties.setProperty("database.query.default.depth", "1");
        properties.setProperty("database.query.max.depth", "100");
        properties.setProperty("database.transaction.timeout", "300");

        // Остальные настройки без изменений...
        // Persistence Configuration
        properties.setProperty("persistence.snapshot.file", "database.snapshot");
        properties.setProperty("persistence.aof.file", "commands.aof");
        properties.setProperty("persistence.auto.save.interval", "5");
        properties.setProperty("persistence.snapshot.compression", "true");

        // Ollama Configuration
        properties.setProperty("rag.ollama.server.host", "localhost");
        properties.setProperty("rag.ollama.server.port", "11434");

        // Embedding Configuration
        properties.setProperty("rag.embedding.model", "all-minilm:22m");
        properties.setProperty("rag.embedding.host", "localhost");
        properties.setProperty("rag.embedding.server.port", "11434");
        properties.setProperty("rag.similarity.threshold", "0.85");
        properties.setProperty("rag.chunking.max.size", "1000");
        properties.setProperty("rag.chunking.min.size", "100");
        properties.setProperty("rag.search.default.topk", "10");
        properties.setProperty("rag.search.max.topk", "50");

        // Logging Configuration
        properties.setProperty("logging.level.ru.miacomsoft.mumpsdb", "INFO");
        properties.setProperty("logging.file.path", "logs/mumpsdb.log");
        properties.setProperty("logging.file.max-size", "10MB");
        properties.setProperty("logging.file.max-history", "7");

        // Performance Configuration
        properties.setProperty("cache.enabled", "true");
        properties.setProperty("cache.max.size", "10000");
        properties.setProperty("cache.expiration.minutes", "60");
        properties.setProperty("thread.pool.size", "20");
        properties.setProperty("query.timeout.seconds", "30");

        // Client Configuration
        properties.setProperty("client.welcome.message", "Welcome to MUMPS-like Database Server");
        properties.setProperty("client.command.prompt", ">");
        properties.setProperty("client.max.command.length", "8192");
        properties.setProperty("client.history.size", "1000");

        // Backup Configuration
        properties.setProperty("backup.enabled", "false");
        properties.setProperty("backup.directory", "backups");
        properties.setProperty("backup.schedule", "0 2 * * *");
        properties.setProperty("backup.retention.days", "7");
    }

    public Properties getProperties() {
        return properties;
    }

    public String getDbUrl() {
        return properties.getProperty("spring.datasource.url");
    }

    public String getOllamaUrl() {
        return String.format("http://%s:%s",
                properties.getProperty("rag.ollama.server.host"),
                properties.getProperty("rag.ollama.server.port"));
    }

    public String getEmbeddingOllamaUrl() {
        return String.format("http://%s:%s",
                properties.getProperty("rag.embedding.host"),
                properties.getProperty("rag.embedding.server.port"));
    }

    public String getEmbeddingServiceUrl() {
        return String.format("http://%s:%s",
                properties.getProperty("rag.embedding.host"),
                properties.getProperty("rag.embedding.server.port"));
    }

    public int getServerPort() {
        return Integer.parseInt(properties.getProperty("server.port", "9090"));
    }

    public String getSnapshotFile() {
        return properties.getProperty("persistence.snapshot.file", "database.snapshot");
    }

    public String getAofFile() {
        return properties.getProperty("persistence.aof.file", "commands.aof");
    }

    public int getAutoSaveInterval() {
        return Integer.parseInt(properties.getProperty("persistence.auto.save.interval", "5"));
    }

    public Properties loadPropertiesFromClasspath(String fileName) {
        Properties props = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                return null;
            }
            props.load(input);
        } catch (IOException ex) {
            System.err.println("Error loading properties from classpath: " + ex.getMessage());
            return null;
        }
        return props;
    }
}