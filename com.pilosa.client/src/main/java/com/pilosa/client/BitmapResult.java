package com.pilosa.client;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BitmapResult {
    private Map<String, Object> attributes;
    private List<Integer> bits;

    public BitmapResult() {
        this.attributes = new HashMap<>(0);
        this.bits = new ArrayList<>(0);
    }

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

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.attributes)
                .append(this.bits)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BitmapResult)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        BitmapResult rhs = (BitmapResult) obj;
        return this.attributes.equals(rhs.attributes) && this.bits.equals(rhs.bits);
    }
}
