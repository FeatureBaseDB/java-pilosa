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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public final class FieldRow {
    public static FieldRow create(String fieldName, long rowID) {
        return new FieldRow(fieldName, rowID, "");
    }
    public static FieldRow create(String fieldName, String rowKey) {
        return new FieldRow(fieldName, 0, rowKey);
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public long getRowID() {
        return this.rowID;
    }

    public String getRowKey() {
        return this.rowKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof FieldRow)) {
            return false;
        }
        FieldRow rhs = (FieldRow) obj;
        return new EqualsBuilder()
                .append(this.fieldName, rhs.fieldName)
                .append(this.rowID, rhs.rowID)
                .append(this.rowKey, rhs.rowKey)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.fieldName)
                .append(this.rowID)
                .append(this.rowKey)
                .toHashCode();
    }

    @Override
    public String toString() {
        return String.format("FieldRow(field=%s, rowID=%d, rowKey=%s)",
                this.fieldName, this.rowID, this.rowKey);
    }

    static FieldRow fromInternal(Internal.FieldRow q) {
        return new FieldRow(q.getField(), q.getRowID(), q.getRowKey());
    }

    private FieldRow(String fieldName, long rowID, String rowKey) {
        this.fieldName = fieldName;
        this.rowID = rowID;
        this.rowKey = rowKey;
    }

    private final String fieldName;
    private final long rowID;
    private final String rowKey;
}
