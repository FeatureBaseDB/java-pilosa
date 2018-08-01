/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package com.pilosa.client;

import com.pilosa.client.orm.Record;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Column implements Record {
    public static final Column DEFAULT = Column.defaultColumn();

    public static Column create(long rowID, long columnID) {
        return create(rowID, columnID, "", "", 0);
    }

    public static Column create(long rowID, long columnID, long timestamp) {
        return create(rowID, columnID, "", "", timestamp);
    }

    public static Column create(long rowID, String columnKey) {
        return create(rowID, 0, columnKey, "", 0);
    }

    public static Column create(long rowID, String columnKey, long timestamp) {
        return create(rowID, 0, columnKey, "", timestamp);
    }

    public static Column create(String rowKey, long columnID) {
        return create(0, columnID, rowKey, "", 0);
    }

    public static Column create(String rowKey, long columnID, long timestamp) {
        return create(0, columnID, rowKey, "", timestamp);
    }

    public static Column create(String rowKey, String columnKey) {
        return create(0, 0, rowKey, columnKey, 0);
    }

    public static Column create(String rowKey, String columnKey, long timestamp) {
        return create(0, 0, rowKey, columnKey, timestamp);
    }

    @SuppressWarnings("WeakerAccess")
    public long getRowID() {
        return this.rowID;
    }

    @SuppressWarnings("WeakerAccess")
    public long getColumnID() {
        return this.columnID;
    }

    @SuppressWarnings("WeakerAccess")
    public String getRowKey() {
        return this.rowKey;
    }

    @SuppressWarnings("WeakerAccess")
    public String getColumnKey() {
        return this.columnKey;
    }

    @SuppressWarnings("WeakerAccess")
    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public long shard(long shardWidth) {
        return this.columnID / shardWidth;
    }

    @Override
    public int compareTo(Record other) {
        Column column = (Column) other;
        // TODO: check column
        if (this.rowID == column.rowID) {
            if (this.columnID == column.columnID) {
                return 0;
            }
            return (this.columnID < column.columnID) ? -1 : 1;
        } else return (this.rowID < column.rowID) ? -1 : 1;
    }

    @Override
    public boolean isDefault() {
        return this.defaultColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Column)) {
            return false;
        }

        Column column = (Column) o;
        return this.defaultColumn == column.defaultColumn ||
                this.rowID == column.rowID &&
                        this.columnID == column.columnID &&
                        this.rowKey.equals(column.rowKey) &&
                        this.columnKey.equals(column.columnKey) &&
                        this.timestamp == column.timestamp;
    }

    @Override
    public int hashCode() {
        if (this.defaultColumn) {
            return new HashCodeBuilder(31, 47).append(true).toHashCode();
        }
        return new HashCodeBuilder(31, 47)
                .append(this.rowID)
                .append(this.columnID)
                .append(this.rowKey)
                .append(this.columnKey)
                .append(this.timestamp)
                .toHashCode();
    }

    @Override
    public String toString() {
        if (this.defaultColumn) {
            return "(default column)";
        }
        return String.format("%d:%d %s:%s [%d]",
                this.rowID, this.columnID, this.rowKey, this.columnKey, this.timestamp);
    }

    private Column() {
    }

    private static Column defaultColumn() {
        Column column = new Column();
        column.defaultColumn = true;
        return column;
    }

    private static Column create(long rowID, long columnID, String rowKey, String columnKey, long timestamp) {
        Column column = new Column();
        column.rowID = rowID;
        column.columnID = columnID;
        column.rowKey = rowKey;
        column.columnKey = columnKey;
        column.timestamp = timestamp;
        return column;
    }

    long rowID = 0;
    long columnID = 0;
    String rowKey = "";
    String columnKey = "";
    long timestamp = 0;
    boolean defaultColumn = false;
}
