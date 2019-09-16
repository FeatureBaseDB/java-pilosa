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

public class FieldValue implements Record {
    public static final FieldValue DEFAULT = FieldValue.defaultFieldValue();

    public static FieldValue create(long columnID, long value) {
        return create(columnID, "", value);
    }

    public static FieldValue create(String columnKey, long value) {
        return create(0, columnKey, value);
    }

    public long getColumnID() {
        return this.columnID;
    }

    public String getColumnKey() {
        return this.columnKey;
    }

    public long getValue() {
        return this.value;
    }

    @Override
    public long shard(long shardWidth) {
        return this.columnID / shardWidth;
    }

    @Override
    public boolean isDefault() {
        return this.defaultFieldValue;
    }

    @Override
    public int compareTo(Record record) {
        // NOTE: This is supposed to be used only during importing !!!
        FieldValue fieldValue = (FieldValue) record;
        // TODO: check field value
        // We check only the columnID, since columnKey is not used for sorting during import.
        if (this.columnID == fieldValue.columnID) {
            return 0;
        }
        return (this.columnID < fieldValue.columnID) ? -1 : 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof FieldValue)) {
            return false;
        }

        FieldValue other = (FieldValue) o;
        return this.defaultFieldValue == other.defaultFieldValue &&
                this.columnID == other.columnID &&
                this.columnKey.equals(other.columnKey) &&
                this.value == other.value;
    }

    @Override
    public int hashCode() {
        if (this.defaultFieldValue) {
            return new HashCodeBuilder(31, 47).append(true).toHashCode();
        }
        return new HashCodeBuilder(31, 47)
                .append(this.columnID)
                .append(this.columnKey)
                .append(this.value)
                .toHashCode();
    }

    @Override
    public String toString() {
        if (this.defaultFieldValue) {
            return "(default field value)";
        }
        return String.format("%d %s = %d",
                this.columnID, this.columnKey, this.value);
    }

    private FieldValue() {

    }

    private static FieldValue defaultFieldValue() {
        FieldValue fieldValue = new FieldValue();
        fieldValue.defaultFieldValue = true;
        return fieldValue;
    }

    private static FieldValue create(long columnID, String columnKey, long value) {
        FieldValue fieldValue = new FieldValue();
        fieldValue.columnID = columnID;
        fieldValue.columnKey = columnKey;
        fieldValue.value = value;
        return fieldValue;
    }

    long columnID = 0;
    String columnKey = "";
    long value = 0;
    boolean defaultFieldValue = false;
}
