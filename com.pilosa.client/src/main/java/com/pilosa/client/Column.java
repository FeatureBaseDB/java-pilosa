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

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Column {
    public static final Column DEFAULT = Column.defaultColumn();

    public static Column create(long rowID, long columnID) {
        return Column.create(rowID, columnID, 0);
    }

    public static Column create(long rowID, long columnID, long timestamp) {
        Column column = new Column();
        column.rowID = rowID;
        column.columnID = columnID;
        column.timestamp = timestamp;
        return column;
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
    public long getTimestamp() {
        return this.timestamp;
    }

    public boolean isDefaultColumn() {
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
                .append(this.timestamp)
                .toHashCode();
    }

    @Override
    public String toString() {
        if (this.defaultColumn) {
            return "(default column)";
        }
        return String.format("%d:%d[%d]", this.rowID, this.columnID, this.timestamp);
    }

    private Column() {
    }

    private static Column defaultColumn() {
        Column column = new Column();
        column.defaultColumn = true;
        return column;
    }

    long rowID = 0;
    long columnID = 0;
    long timestamp = 0;
    boolean defaultColumn = false;
}
