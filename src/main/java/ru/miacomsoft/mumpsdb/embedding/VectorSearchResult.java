package ru.miacomsoft.mumpsdb.embedding;

public class VectorSearchResult {
    private final String global;
    private final Object[] path;
    private final Object value;
    private final float[] embedding;
    private final double similarity;

    public VectorSearchResult(String global, Object[] path, Object value,
                              float[] embedding, double similarity) {
        this.global = global;
        this.path = path;
        this.value = value;
        this.embedding = embedding;
        this.similarity = similarity;
    }

    // Getters
    public String getGlobal() { return global; }
    public Object[] getPath() { return path; }
    public Object getValue() { return value; }
    public float[] getEmbedding() { return embedding; }
    public double getSimilarity() { return similarity; }

    @Override
    public String toString() {
        return String.format("VectorSearchResult{global='%s', path=%s, similarity=%.4f, value=%s}",
                global, java.util.Arrays.toString(path), similarity, value);
    }
}