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
import com.pilosa.client.orm.Index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class ShardColumns {
    public static ShardColumns create(final Field field, final long shard) {
        return new ShardColumns(field, shard);
    }

    public Index getIndex() {
        return this.field.getIndex();
    }

    public long getShard() {
        return this.shard;
    }

    public List<Column> getColumns() {
        if (!this.sorted) {
            Collections.sort(columns, shardComparator);
            this.sorted = true;
        }
        return this.columns;
    }

    public void add(Column column) {
        this.columns.add(column);
    }

    public void clear() {
        this.columns.clear();
    }

    Internal.ImportRequest convertToImportRequest() {
        List<Long> rowIDs = new ArrayList<>(columns.size());
        List<Long> columnIDs = new ArrayList<>(columns.size());
        List<Long> timestamps = new ArrayList<>(columns.size());
        for (Column column : columns) {
            rowIDs.add(column.getRowID());
            columnIDs.add(column.getColumnID());
            timestamps.add(column.getTimestamp());
        }
        return Internal.ImportRequest.newBuilder()
                .setIndex(this.field.getIndex().getName())
                .setField(this.field.getName())
                .setShard(shard)
                .addAllRowIDs(rowIDs)
                .addAllColumnIDs(columnIDs)
                .addAllTimestamps(timestamps)
                .build();
    }

    ShardColumns(final Field field, final long shard) {
        this.field = field;
        this.shard = shard;
        this.columns = new ArrayList<>();
    }

    private static Comparator<Column> shardComparator = new BitComparator();
    private final Field field;
    private final long shard;
    private List<Column> columns;
    private boolean sorted = false;
}
