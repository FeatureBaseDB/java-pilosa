package com.pilosa.client;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents a result from {@link com.pilosa.client.orm.Frame#topN(long)} call.
 *
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public final class CountResultItem {
    private long id;
    private long count;

    CountResultItem() {
    }

    CountResultItem(long id, long count) {
        this.id = id;
        this.count = count;
    }

    static CountResultItem fromInternal(Internal.Pair pair) {
        return new CountResultItem(pair.getKey(), pair.getCount());
    }

    /**
     * Returns the row ID.
     *
     * @return row ID
     */
    public long getID() {
        return this.id;
    }

    /**
     * Returns the count of column IDs where this bitmap item is 1.
     *
     * @return count of column IDs where this bitmap item is 1
     */
    public long getCount() {
        return this.count;
    }

    @Override
    public String toString() {
        return String.format("CountResultItem(key=%d, count=%d)", this.id, this.count);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.id)
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
        return this.id == rhs.id && this.count == rhs.count;
    }

}
