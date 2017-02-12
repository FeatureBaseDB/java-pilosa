package com.pilosa.client;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a result from Bitmap, Union, Intersect, Difference and Range PQL calls.
 */
public class BitmapResult {
    private Map<String, Object> attributes;
    private List<Integer> bits;

    BitmapResult() {
        this.attributes = new HashMap<>(0);
        this.bits = new ArrayList<>(0);
    }

    BitmapResult(Map<String, Object> attributes, List<Integer> bits) {
        this.attributes = attributes;
        this.bits = bits;
    }

    /**
     * Returns the attributes of the reply.
     *
     * @return attributes
     */
    public Map<String, Object> getAttributes() {
        return this.attributes;
    }

    /**
     * Returns the bits of the reply.
     *
     * @return list of profile IDs where the corresponding bit is 1
     */
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
