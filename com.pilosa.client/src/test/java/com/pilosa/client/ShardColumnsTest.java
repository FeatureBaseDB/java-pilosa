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

import com.google.protobuf.InvalidProtocolBufferException;
import com.pilosa.client.orm.FieldOptions;
import com.pilosa.client.orm.Index;
import com.pilosa.client.orm.IndexOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static com.pilosa.client.Internal.ImportRequest.parseFrom;
import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ShardColumnsTest {

    private static final String INDEX_NAME = "test-index";
    private static final String FIELD_NAME = "test-field";

    @Test
    public void testToImportRequestCsvIndexAndFieldKeys() throws InvalidProtocolBufferException {
        List<Column> columns = Arrays.asList(Column.create("row-a", "column-a"),
                Column.create("row-b", "column-b"));
        ShardColumns shardColumns = buildShardColumns(true, true, false, columns);
        Internal.ImportRequest internalImportRequest = toShardColumnsInternalImportRequest(shardColumns);
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(internalImportRequest.getRowKeys(i), columns.get(i).getRowKey());
            assertEquals(internalImportRequest.getColumnKeys(i), columns.get(i).getColumnKey());
        }
        assertEquals(FIELD_NAME, internalImportRequest.getField());
        assertEquals(INDEX_NAME, internalImportRequest.getIndex());
    }

    @Test
    public void testToImportRequestCsvIndexKeysNoFieldKeys() throws InvalidProtocolBufferException {
        List<Column> columns = Arrays.asList(Column.create(1L, "column-a"),
                Column.create(2L, "column-b"));
        ShardColumns shardColumns = buildShardColumns(true, false, false, columns);
        Internal.ImportRequest internalImportRequest = toShardColumnsInternalImportRequest(shardColumns);
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(internalImportRequest.getRowIDs(i), columns.get(i).getRowID());
            assertEquals(internalImportRequest.getColumnKeys(i), columns.get(i).getColumnKey());
        }
        assertEquals(FIELD_NAME, internalImportRequest.getField());
        assertEquals(INDEX_NAME, internalImportRequest.getIndex());
    }

    @Test
    public void testToImportRequestCsvNoIndexKeysWithFieldKeys() throws InvalidProtocolBufferException {
        List<Column> columns = Arrays.asList(Column.create("row-a", 100L),
                Column.create("row-b", 101L));
        ShardColumns shardColumns = buildShardColumns(false, true, false, columns);
        Internal.ImportRequest internalImportRequest = toShardColumnsInternalImportRequest(shardColumns);
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(internalImportRequest.getRowKeys(i), columns.get(i).getRowKey());
            assertEquals(internalImportRequest.getColumnIDs(i), columns.get(i).getColumnID());
        }
        assertEquals(FIELD_NAME, internalImportRequest.getField());
        assertEquals(INDEX_NAME, internalImportRequest.getIndex());
    }

    @Test
    public void testToImportRequestCsvWithNoKeys() throws InvalidProtocolBufferException {
        List<Column> columns = Arrays.asList(Column.create(1L, 100L),
                Column.create(2L, 101L));
        ShardColumns shardColumns = buildShardColumns(false, false, false, columns);
        Internal.ImportRequest internalImportRequest = toShardColumnsInternalImportRequest(shardColumns);
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(internalImportRequest.getRowIDs(i), columns.get(i).getRowID());
            assertEquals(internalImportRequest.getColumnIDs(i), columns.get(i).getColumnID());
        }
        assertEquals(FIELD_NAME, internalImportRequest.getField());
        assertEquals(INDEX_NAME, internalImportRequest.getIndex());
    }

    private static ShardColumns buildShardColumns(boolean indexKeys, boolean fieldKeys,
                                                  boolean isRoaring, List<Column> columns) {
        ImportOptions options = ImportOptions.builder()
                .setRoaring(isRoaring)
                .build();
        return addColumns(ShardColumns.create(
                Index.create(INDEX_NAME, IndexOptions.builder().keys(indexKeys).build())
                        .field(FIELD_NAME, FieldOptions.builder().keys(fieldKeys).build()),
                1L, ClientOptions.DEFAULT_SHARD_WIDTH, options), columns);
    }

    private static ShardColumns addColumns(ShardColumns shardColumns, List<Column> columns) {
        for (Column column : columns) {
            shardColumns.add(column);
        }
        return shardColumns;
    }

    private static Internal.ImportRequest toShardColumnsInternalImportRequest(ShardColumns shardColumns) throws InvalidProtocolBufferException {
        return parseFrom(shardColumns.toImportRequest().payload);
    }
}
