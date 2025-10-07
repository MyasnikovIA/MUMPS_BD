package com.mumpsdb.embedding;

import com.mumpsdb.ConfigLoader;

import java.util.List;

public class EmbeddingService {
    private final SemanticChunker semanticChunker;
    private final ConfigLoader configLoader;
    private final boolean embeddingEnabled;

    public EmbeddingService() {
        this.configLoader = new ConfigLoader();
        this.embeddingEnabled = configLoader.isAutoEmbeddingEnabled();

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
        return semanticChunker.getEmbedding(text);
    }

    public List<SemanticChunker.Chunk> chunkText(String text, int maxChunkSize) throws Exception {
        if (!embeddingEnabled) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }
        return semanticChunker.semanticChunking(text, maxChunkSize);
    }

    public double calculateSimilarity(float[] embedding1, float[] embedding2) {
        if (!embeddingEnabled) {
            throw new IllegalStateException("Embedding functionality is disabled");
        }
        return semanticChunker.cosineSimilarity(embedding1, embedding2);
    }

    public boolean isEmbeddingEnabled() {
        return embeddingEnabled;
    }
}