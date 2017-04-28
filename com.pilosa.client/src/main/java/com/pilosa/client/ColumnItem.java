package com.pilosa.client;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Map;

/**
 * Contains data about a column.
 * <p>
 * Column data is returned from {@link QueryResponse#getColumns()} method.
 * They are only returned if {@link QueryOptions.Builder#setColumns(boolean)} was set to <code>true</code>.
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/">Data Model</a>
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public final class ColumnItem {
    private long id;
    private Map<String, Object> attributes;

    ColumnItem() {
    }

    ColumnItem(long id, Map<String, Object> attributes) {
        this.id = id;
        this.attributes = attributes;
    }

    static ColumnItem fromInternal(Internal.ColumnAttrSet column) {
        return new ColumnItem(column.getID(), Util.protobufAttrsToMap(column.getAttrsList()));
    }

    /**
     * Returns column ID
     *
     * @return column ID
     */
    public long getID() {
        return this.id;
    }

    public Map<String, Object> getAttributes() {
        return this.attributes;
    }

    @Override
    public String toString() {
        return String.format("ColumnItem(id=%d, attrs=%s)", this.id, this.attributes);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.id)
                .append(this.attributes)
                .toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ColumnItem)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        ColumnItem rhs = (ColumnItem) obj;
        return new EqualsBuilder()
                .append(this.id, rhs.id)
                .append(this.attributes, rhs.attributes)
                .isEquals();
    }
}
