package com.pilosa.client;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Map;

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
     * Returns column ID.
     *
     * @return column ID
     */
    public long getID() {
        return this.id;
    }

    /**
     * Returns column attributes.
     *
     * @return column attributes
     */
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
