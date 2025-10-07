package ru.miacomsoft.mumpsdb.embedding;

import ru.miacomsoft.mumpsdb.ConfigLoader;
import ru.miacomsoft.mumpsdb.monitoring.MetricsCollector;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class EmbeddingService {
    private final SemanticChunker semanticChunker;
    private final ConfigLoader configLoader;
    private final boolean embeddingEnabled;
    private final MetricsCollector metrics;

    public EmbeddingService() {
        this.configLoader = new ConfigLoader();
        this.embeddingEnabled = configLoader.isAutoEmbeddingEnabled();
        this.metrics = MetricsCollector.getInstance();

        if (embeddingEnabled) {
            String ollamaUrl = configLoader.getEmbeddingOllamaUrl();
            String embeddingModel = configLoader.getProperties().getProperty("rag.embedding.model", "all-minilm:22m");
            double similarityThreshold = Double.parseDouble(
                    configLoader.getProperties().getProperty("rag.similarity.threshold", "0.9")
            );

            this.semanticChunker = new SemanticChunker(ollamaUrl, embeddingModel, similarityThreshold);
        } else {
            this.semanticChunker = null;
        }
    }

    public float[] getEmbedding(String text) throws Exception {
        if (!embeddingEnabled) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }

        long startTime = System.currentTimeMillis();
        try {
            validateTextForEmbedding(text);
            float[] embedding = semanticChunker.getEmbedding(text);
            metrics.incrementCounter("embedding_generated");
            return embedding;
        } catch (Exception e) {
            metrics.incrementCounter("embedding_errors");
            throw new EmbeddingException("Failed to generate embedding for text: " +
                    (text != null ? text.substring(0, Math.min(50, text.length())) : "null"), e);
        } finally {
            metrics.recordOperationTime("embedding_generation", startTime);
        }
    }

    public List<SemanticChunker.Chunk> chunkText(String text, int maxChunkSize) throws Exception {
        if (!embeddingEnabled) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }

        long startTime = System.currentTimeMillis();
        try {
            validateTextForChunking(text, maxChunkSize);
            List<SemanticChunker.Chunk> chunks = semanticChunker.semanticChunking(text, maxChunkSize);
            metrics.incrementCounter("text_chunked", chunks.size());
            return chunks;
        } catch (Exception e) {
            metrics.incrementCounter("chunking_errors");
            throw new EmbeddingException("Failed to chunk text", e);
        } finally {
            metrics.recordOperationTime("text_chunking", startTime);
        }
    }

    public double calculateSimilarity(float[] embedding1, float[] embedding2) {
        if (!embeddingEnabled) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }

        try {
            validateEmbedding(embedding1);
            validateEmbedding(embedding2);
            return semanticChunker.cosineSimilarity(embedding1, embedding2);
        } catch (Exception e) {
            metrics.incrementCounter("similarity_calculation_errors");
            throw new EmbeddingException("Failed to calculate similarity", e);
        }
    }

    private void validateTextForEmbedding(String text) {
        if (text == null) {
            throw new IllegalArgumentException("Text cannot be null for embedding");
        }
        if (text.trim().isEmpty()) {
            throw new IllegalArgumentException("Text cannot be empty for embedding");
        }
        if (text.length() > 10000) {
            throw new IllegalArgumentException("Text too long for embedding: " + text.length() + " characters");
        }
    }

    private void validateTextForChunking(String text, int maxChunkSize) {
        if (text == null) {
            throw new IllegalArgumentException("Text cannot be null for chunking");
        }
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("Max chunk size must be positive");
        }
        if (maxChunkSize > 100000) {
            throw new IllegalArgumentException("Max chunk size too large: " + maxChunkSize);
        }
    }

    private void validateEmbedding(float[] embedding) {
        if (embedding == null) {
            throw new IllegalArgumentException("Embedding cannot be null");
        }
        if (embedding.length == 0) {
            throw new IllegalArgumentException("Embedding cannot be empty");
        }
        if (embedding.length > 10000) {
            throw new IllegalArgumentException("Embedding dimension too large: " + embedding.length);
        }

        // Проверка на NaN значения
        for (float value : embedding) {
            if (Float.isNaN(value)) {
                throw new IllegalArgumentException("Embedding contains NaN values");
            }
        }
    }

    public boolean isEmbeddingEnabled() {
        return embeddingEnabled;
    }

    public static class EmbeddingException extends RuntimeException {
        public EmbeddingException(String message) {
            super(message);
        }

        public EmbeddingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}