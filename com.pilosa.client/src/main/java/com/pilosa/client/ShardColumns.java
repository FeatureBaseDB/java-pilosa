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

import com.google.protobuf.ByteString;
import com.pilosa.client.orm.Field;
import com.pilosa.client.orm.FieldType;
import com.pilosa.client.orm.Record;
import com.pilosa.roaring.Bitmap;

import java.text.SimpleDateFormat;
import java.util.*;

class ShardColumns implements ShardRecords {
    public static ShardColumns create(final Field field, long shard, long shardWidth, ImportOptions options) {
        return new ShardColumns(field, shard, shardWidth, options.isRoaring(), options.isClear());
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
        return ImportRequest.createCSVImport(field, requestBuilder.build().toByteArray(), this.clear_);
    }

    ImportRequest toRoaringImportRequest() {
        Map<String, Bitmap> views;
        if (field.getOptions().getFieldType() == FieldType.TIME) {
            views = columnsToBitmap(field.getOptions().getTimeQuantum());
        }
        else {
            views = columnsToBitmap();
        }
        Internal.ImportRoaringRequest.Builder reqBuilder = Internal.ImportRoaringRequest.newBuilder();
        for (Map.Entry<String, Bitmap> entry : views.entrySet()) {
            byte[] bmpData = entry.getValue().serialize().array();
            ByteString data = ByteString.copyFrom(bmpData);
            Internal.ImportRoaringRequestView view = Internal.ImportRoaringRequestView.newBuilder()
                    .setName(entry.getKey())
                    .setData(data)
                    .build();
            reqBuilder.addViews(view);
        }
        reqBuilder.setClear(this.clear_);
        byte[] payload = reqBuilder.build().toByteArray();
        return ImportRequest.createRoaringImport(this.field, this.shard, payload, this.clear_);
    }

    ShardColumns(final Field field, long shard, long shardWidth, boolean roaring, boolean clear) {
        this.field = field;
        this.shard = shard;
        this.shardWidth = shardWidth;
        this.columns = new ArrayList<>();
        this.roaring = roaring;
        this.clear_ = clear;
    }

    private Map<String, Bitmap> columnsToBitmap() {
        long shardWidth = this.shardWidth;
        Map<String, Bitmap> result = new HashMap<>(1);
        Bitmap bmp = new Bitmap();
        for (Column b : this.columns) {
            bmp.add(b.rowID * shardWidth + (b.columnID % shardWidth));
        }
        result.put("", bmp);
        return result;
    }

    private Map<String, Bitmap> columnsToBitmap(TimeQuantum timeQuantum) {
        long shardWidth = this.shardWidth;
        Map<String, Bitmap> views = new HashMap<>();
        Bitmap standard = new Bitmap();
        for (Column b : this.columns) {
            long bit = b.rowID * shardWidth + (b.columnID % shardWidth);
            standard.add(bit);
            String[] viewNames = viewsByTime(b.timestamp, timeQuantum);
            for (String viewName : viewNames) {
                Bitmap bmp = views.get(viewName);
                if (bmp == null) {
                    bmp = new Bitmap();
                    views.put(viewName, bmp);
                }
                bmp.add(bit);
            }
        }
        views.put("", standard); // standard view
        return views;
    }

    private String[] viewsByTime(long timestamp, TimeQuantum timeQuantum) {
        String tqs = timeQuantum.toString();
        String[] result = new String[tqs.length()];
        for (int i = 0; i < tqs.length(); i++) {
            result[i] = viewByTimeUnit(timestamp, tqs.charAt(i));
        }
        return result;
    }

    private String viewByTimeUnit(long timestamp, char c) {
        SimpleDateFormat fmt = this.timeFormats.get(c);
        if (fmt == null) {
            return "";
        }
        return fmt.format(new Date(timestamp*1000));
    }

    private Map<Character, SimpleDateFormat> timeFormats = new HashMap<>(4);

    {
        timeFormats.put('Y', new SimpleDateFormat("yyyy"));
        timeFormats.put('M', new SimpleDateFormat("MM"));
        timeFormats.put('D', new SimpleDateFormat("dd"));
        timeFormats.put('H', new SimpleDateFormat("mm"));
    }

    private final Field field;
    private final long shard;
    private final long shardWidth;
    private List<Column> columns;
    private boolean sorted = false;
    private final boolean roaring;
    private final boolean clear_;
}
