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

import static com.pilosa.client.Internal.ImportValueRequest.parseFrom;
import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ShardFieldValuesTest {

    private static final String INDEX_NAME = "test-index";
    private static final String FIELD_NAME = "test-field";

    @Test
    public void testToImportRequestCsvIndexKeys() throws InvalidProtocolBufferException {
        List<FieldValue> fieldValues = Arrays.asList(FieldValue.create("column-a", 10L),
                FieldValue.create("column-b", 20L));
        ShardFieldValues shardFieldValues = buildShardFieldValues(true, false, fieldValues);
        Internal.ImportValueRequest internalImportValueRequest = toShardFieldValuesInternalImportValueRequest(shardFieldValues);
        for (int i = 0; i < fieldValues.size(); i++) {
            assertEquals(internalImportValueRequest.getColumnKeys(i), fieldValues.get(i).getColumnKey());
            assertEquals(internalImportValueRequest.getValues(i), fieldValues.get(i).getValue());
        }
        assertEquals(FIELD_NAME, internalImportValueRequest.getField());
        assertEquals(INDEX_NAME, internalImportValueRequest.getIndex());
    }

    @Test
    public void testToImportRequestCsvNoIndexKeys() throws InvalidProtocolBufferException {
        List<FieldValue> fieldValues = Arrays.asList(FieldValue.create(1L, 10L),
                FieldValue.create(2L, 20L));
        ShardFieldValues shardFieldValues = buildShardFieldValues(false, false, fieldValues);
        Internal.ImportValueRequest internalImportValueRequest = toShardFieldValuesInternalImportValueRequest(shardFieldValues);
        for (int i = 0; i < fieldValues.size(); i++) {
            assertEquals(internalImportValueRequest.getColumnIDs(i), fieldValues.get(i).getColumnID());
            assertEquals(internalImportValueRequest.getValues(i), fieldValues.get(i).getValue());
        }
        assertEquals(FIELD_NAME, internalImportValueRequest.getField());
        assertEquals(INDEX_NAME, internalImportValueRequest.getIndex());
    }

    private static ShardFieldValues buildShardFieldValues(boolean indexKeys,
                                                  boolean isRoaring, List<FieldValue> fieldValues) {
        ImportOptions options = ImportOptions.builder()
                .setRoaring(isRoaring)
                .build();
        return addFieldValues(ShardFieldValues.create(
                Index.create(INDEX_NAME, IndexOptions.builder().setKeys(indexKeys).build())
                        .field(FIELD_NAME, FieldOptions.builder().fieldInt(-100, 100).build()),
                0L, options), fieldValues);
    }

    private static ShardFieldValues addFieldValues(ShardFieldValues shardFieldValues, List<FieldValue> fieldValues) {
        for (FieldValue fieldValue : fieldValues) {
            shardFieldValues.add(fieldValue);
        }
        return shardFieldValues;
    }

    private static Internal.ImportValueRequest toShardFieldValuesInternalImportValueRequest(ShardFieldValues shardFieldValues) throws InvalidProtocolBufferException {
        return parseFrom(shardFieldValues.toImportRequest().payload);
    }
}
