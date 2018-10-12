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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.pilosa.client.orm.Field;
import com.pilosa.client.orm.Record;
import com.pilosa.roaring.Bitmap;

class ShardColumns implements ShardRecords {
    public static ShardColumns create(final Field field, long shard, long shardWidth, boolean roaring) {
        return new ShardColumns(field, shard, shardWidth, roaring);
    }

    @Override
    public long getShard() {
        return this.shard;
    }

    @Override
    public String getIndexName() {
        return null;
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
    public void add(Record record) {
        Column column = (Column) record;
        // TODO: check column
        this.columns.add(column);
    }

    @Override
    public int size() {
        return this.columns.size();
    }

    @Override
    public void clear() {
        this.columns.clear();
    }

    @Override
    public ImportRequest toImportRequest() {
        if (this.roaring && !isIndexKeys() && !isFieldKeys()) {
            return toRoaringImportRequest();
        }
        return toCSVImportRequest();
    }

    public ImportRequest toCSVImportRequest() {
        if (!sorted) {
            Collections.sort(columns);
            sorted = true;
        }

        Internal.ImportRequest.Builder requestBuilder = Internal.ImportRequest.newBuilder()
                .setIndex(field.getIndex().getName())
                .setField(field.getName())
                .setShard(shard);

        if (isIndexKeys() && isFieldKeys()) {
            for (Column column : columns){
                requestBuilder.addRowKeys(column.rowKey);
                requestBuilder.addColumnKeys(column.columnKey);
                requestBuilder.addTimestamps(column.timestamp);
            }
        }
        else if(!isIndexKeys() && isFieldKeys()) {
            for (Column column : columns){
                requestBuilder.addRowKeys(column.rowKey);
                requestBuilder.addColumnIDs(column.columnID);
                requestBuilder.addTimestamps(column.timestamp);
            }
        }
        else if(isIndexKeys() && !isFieldKeys()){
            for (Column column : columns){
                requestBuilder.addRowIDs(column.rowID);
                requestBuilder.addColumnKeys(column.columnKey);
                requestBuilder.addTimestamps(column.timestamp);
            }
        }
        else {
            for (Column column : columns){
                requestBuilder.addRowIDs(column.rowID);
                requestBuilder.addColumnIDs(column.columnID);
                requestBuilder.addTimestamps(column.timestamp);
            }
        }
        return ImportRequest.createCSVImport(field, requestBuilder.build().toByteArray());
    }

    ImportRequest toRoaringImportRequest() {
        long shardWidth = this.shardWidth;
        Bitmap bmp = new Bitmap();
        for (Column b : this.columns) {
            bmp.add(b.rowID * shardWidth + (b.columnID % shardWidth));
        }
        return ImportRequest.createRoaringImport(this.field, this.shard, bmp.serialize().array());
    }

    ShardColumns(final Field field, long shard, long shardWidth, boolean roaring) {
        this.field = field;
        this.shard = shard;
        this.shardWidth = shardWidth;
        this.columns = new ArrayList<>();
        this.roaring = roaring;
    }

    private final Field field;
    private final long shard;
    private final long shardWidth;
    private List<Column> columns;
    private boolean sorted = false;
    private final boolean roaring;
}
