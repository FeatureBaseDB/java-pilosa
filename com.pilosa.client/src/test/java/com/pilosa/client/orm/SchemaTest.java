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
        // Create schema1
        /*
            schema1
                |___ index11
                |       |___ index11-f1
                |       |___ index11-f2 [keys]
                |
                |___ index12 [keys]
                |       |___ index12-f1
                |
                |___ index13 [keys]
                        |___ index13-f1 [keys]
        */
        Schema schema1 = Schema.defaultSchema();
        Index index11 = makeIndex(schema1, "index11");
        makeField(index11, "index11-f1");
        makeField(index11, "index11-f2", true);
        Index index12 = makeIndex(schema1,  "index12", true);
        makeField(index12, "index12-f1");
        Index index13 = makeIndex(schema1, "index13", true);
        makeField(index13, "index13-f1", true);

        // Create schema2
        /*
            schema2
                |___ index21
                |       |___ index21-f1
                |
                |___ index13 [keys]
                        |___ index13-f2
         */
        Schema schema2 = Schema.defaultSchema();
        Index index21 = makeIndex(schema2, "index21");
        makeField(index21, "index21-f1");

        Index s2Index13 = makeIndex(schema2, "index13", true);
        makeField(s2Index13, "index13-f2");

        // target schema1 - schema2
        /*
            target
                |___ index11
                |       |___ index11-f1
                |       |___ index11-f2 [keys]
                |
                |___ index12 [keys]
                |       |___ index12-f1
                |
                |___ index13 [keys]
                        |___ index13-f1 [keys]
         */
        Schema target = Schema.defaultSchema();
        Index targetIndex11 = makeIndex(target, "index11");
        makeField(targetIndex11, "index11-f1");
        makeField(targetIndex11, "index11-f2", true);
        Index targetIndex12 = makeIndex(target, "index12", true);
        makeField(targetIndex12, "index12-f1");
        Index targetIndex13 = makeIndex(target, "index13", true);
        makeField(targetIndex13, "index13-f1", true);
        // schema1 - schema2
        Schema diff = schema1.diff(schema2);
        assertEquals(target, diff);
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

    private Index makeIndex(Schema schema, String name) {
        return schema.index(name);
    }
    private Index makeIndex(Schema schema, String name, boolean hasKeys) {
        IndexOptions options = IndexOptions.builder()
                .setKeys(true)
                .build();
        return schema.index(name, options);
    }
    private Field makeField(Index index, String name) {
        return index.field(name);
    }
    private Field makeField(Index index, String name, boolean hasKeys) {
        FieldOptions options = FieldOptions.builder()
                .setKeys(true)
                .build();
        return index.field(name, options);
    }
}
