package com.pilosa.client;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Map;

/**
 * Represents a result from Bitmap, Union, Intersect, Difference and Range PQL calls.
 */
public final class BitmapResult {
    private Map<String, Object> attributes;
    private List<Long> bits;

    BitmapResult() {
    }

    BitmapResult(Map<String, Object> attributes, List<Long> bits) {
        this.attributes = attributes;
        this.bits = bits;
    }

    static BitmapResult fromInternal(Internal.Bitmap b) {
        return new BitmapResult(Util.protobufAttrsToMap(b.getAttrsList()), b.getBitsList());
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
    public List<Long> getBits() {
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
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof BitmapResult)) {
            return false;
        }
        BitmapResult rhs = (BitmapResult) obj;
        return new EqualsBuilder()
                .append(this.attributes, rhs.attributes)
                .append(this.bits, rhs.bits)
                .isEquals();
    }
}
