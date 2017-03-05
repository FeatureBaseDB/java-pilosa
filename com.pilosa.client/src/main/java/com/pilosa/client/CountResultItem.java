package com.pilosa.client;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents a result from TopN call.
 */
public final class CountResultItem {
    private long key;
    private long count;

    CountResultItem() {
    }

    CountResultItem(long key, long count) {
        this.key = key;
        this.count = count;
    }

    static CountResultItem fromInternal(Internal.Pair pair) {
        return new CountResultItem(pair.getKey(), pair.getCount());
    }

    /**
     * Returns the bitmap ID.
     *
     * @return bitmap ID
     */
    public long getKey() {
        return this.key;
    }

    /**
     * Returns the count of profile IDs where this bitmap item is 1.
     *
     * @return count of profile IDs where this bitmap item is 1
     */
    public long getCount() {
        return this.count;
    }

    @Override
    public String toString() {
        return String.format("CountResultItem(key=%d, count=%d)", this.key, this.count);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.key)
                .append(this.count)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CountResultItem)) {
            return false;
        }
        CountResultItem rhs = (CountResultItem) obj;
        return this.key == rhs.key && this.count == rhs.count;
    }

}
