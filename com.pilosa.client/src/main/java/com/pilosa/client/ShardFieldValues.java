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

import com.pilosa.client.orm.Field;
import com.pilosa.client.orm.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShardFieldValues implements ShardRecords {
    public static ShardFieldValues create(final Field field, final long shard) {
        return new ShardFieldValues(field, shard);
    }

    @Override
    public long getShard() {
        return this.shard;
    }

    @Override
    public String getIndexName() {
        return this.field.getIndex().getName();
    }

    @Override
    public boolean isIndexKeys() {
        return this.field.getIndex().getOptions().isKeys();
    }

    @Override
    public boolean isFieldKeys() {
        return this.field.getOptions().isKeys();
    }

    @Override
    public int size() {
        return this.fieldValues.size();
    }

    @Override
    public void add(Record record) {
        FieldValue fieldValue = (FieldValue) record;
        // TODO: check fieldValue
        this.fieldValues.add(fieldValue);
    }

    @Override
    public void clear() {
        this.fieldValues.clear();
    }

    @Override
    public ImportRequest toImportRequest() {
        if (!this.sorted) {
            Collections.sort(this.fieldValues);
            this.sorted = true;
        }

        int fieldValueCount = this.fieldValues.size();

        List<Long> values = new ArrayList<>(fieldValueCount);
        Internal.ImportValueRequest.Builder requestBuilder = Internal.ImportValueRequest.newBuilder()
                .setIndex(this.field.getIndex().getName())
                .setField(this.field.getName())
                .setShard(shard);

        if (this.field.getOptions().isKeys()) {
            List<String> columnKeys = new ArrayList<>(fieldValueCount);
            for (FieldValue fieldValue : this.fieldValues) {
                columnKeys.add(fieldValue.columnKey);
                values.add(fieldValue.value);
            }
            requestBuilder.addAllColumnKeys(columnKeys);
            columnKeys = null;
        } else {
            List<Long> columnIDs = new ArrayList<>(fieldValueCount);
            for (FieldValue fieldValue : this.fieldValues) {
                columnIDs.add(fieldValue.columnID);
                values.add(fieldValue.value);
            }
            requestBuilder.addAllColumnIDs(columnIDs);
            columnIDs = null;
        }

        requestBuilder.addAllValues(values);
        values = null;

        return ImportRequest.createCSVImport(this.field, requestBuilder.build().toByteArray());
    }

    ShardFieldValues(final Field field, final long shard) {
        this.field = field;
        this.shard = shard;
        this.fieldValues = new ArrayList<>();
    }

    private final Field field;
    private final long shard;
    private List<FieldValue> fieldValues;
    private boolean sorted = false;

}
