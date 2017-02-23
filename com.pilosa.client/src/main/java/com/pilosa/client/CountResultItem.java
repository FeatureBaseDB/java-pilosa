package com.pilosa.client;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents a result from TopN call.
 */
public class CountResultItem {
    private int key;
    private int count;

    CountResultItem() {
    }

    CountResultItem(int key, int count) {
        this.key = key;
        this.count = count;
    }

    /**
     * Returns the bitmap ID.
     *
     * @return bitmap ID
     */
    public int getKey() {
        return this.key;
    }

    /**
     * Returns the count of profile IDs where this bitmap item is 1.
     *
     * @return count of profile IDs where this bitmap item is 1
     */
    public int getCount() {
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
        if (!(obj instanceof CountResultItem)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        CountResultItem rhs = (CountResultItem) obj;
        return this.key == rhs.key && this.count == rhs.count;
    }

}
