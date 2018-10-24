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

package com.pilosa.client.orm;

import com.pilosa.client.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class SchemaTest {
    @Test
    public void diffTest() {
        Schema schema1 = Schema.defaultSchema();
        Index index11 = schema1.index("diff-index1");
        index11.field("field1-1");
        index11.field("field1-2");
        Index index12 = schema1.index("diff-index2",
                IndexOptions.builder().keys(true).build());
        index12.field("field2-1");

        Schema schema2 = Schema.defaultSchema();
        Index index21 = schema2.index("diff-index1");
        index21.field("another-field");

        Schema targetDiff12 = Schema.defaultSchema();
        Index targetIndex1 = targetDiff12.index("diff-index1");
        targetIndex1.field("field1-1");
        targetIndex1.field("field1-2");
        Index targetIndex2 = targetDiff12.index("diff-index2",
                IndexOptions.builder().keys(true).build());
        targetIndex2.field("field2-1");

        Schema diff12 = schema1.diff(schema2);
        assertEquals(targetDiff12, diff12);
    }

    @Test
    public void addGetIndex() {
        Schema schema1 = Schema.defaultSchema();
        Index index1 = schema1.index("foo");
        Index index2 = schema1.index("foo");
        assertTrue(index1 == index2);
    }

    @Test
    public void indexCopy() {
        Schema schema1 = Schema.defaultSchema();
        Index index1 = schema1.index("foo");
        index1.field("bar");
        Index index2 = schema1.index(index1);
        assertEquals(index1, index2);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = Schema.defaultSchema().equals("foo");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        Schema schema = Schema.defaultSchema();
        schema.index("foo");
        assertEquals(schema, schema);
    }

    @Test
    public void testHashCode() {
        Schema schema1 = Schema.defaultSchema();
        schema1.index("foo");
        Schema schema2 = Schema.defaultSchema();
        schema2.index("foo");
        assertEquals(schema1.hashCode(), schema2.hashCode());
    }
}
