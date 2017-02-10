package com.pilosa.client;

import java.util.List;
import java.util.Map;

public class BitmapResult {
    private Map<String, Object> attributes;
    private List<Integer> bits;

    public BitmapResult(Map<String, Object> attributes, List<Integer> bits) {
        this.attributes = attributes;
        this.bits = bits;
    }

    public Map<String, Object> getAttributes() {
        return this.attributes;
    }

    public List<Integer> getBits() {
        return this.bits;
    }

    @Override
    public String toString() {
        return String.format("BitmapResult(attrs=%s, bits=%s)", this.attributes, this.bits);
    }
}
