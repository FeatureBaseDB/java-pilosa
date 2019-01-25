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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class FieldRowTest {
    @Test
    public void testFieldRow() {
        FieldRow f = FieldRow.create("f1", 42);
        assertEquals("f1", f.getFieldName());
        assertEquals(42, f.getRowID());
        assertEquals("", f.getRowKey());
        assertEquals("FieldRow(field=f1, rowID=42, rowKey=)", f.toString());

        f = FieldRow.create("f1", "forty-two");
        assertEquals("f1", f.getFieldName());
        assertEquals(0, f.getRowID());
        assertEquals("forty-two", f.getRowKey());
        assertEquals("FieldRow(field=f1, rowID=0, rowKey=forty-two)", f.toString());
    }

    @Test
    public void testEquals() {
        FieldRow f = FieldRow.create("f1", 42);
        assertTrue(f.equals(f));
        assertFalse(f.equals(new Integer(42)));
    }

    @Test
    public void testHashCode() {
        FieldRow f1 = FieldRow.create("f1", 42);
        FieldRow f2 = FieldRow.create("f1", 42);
        assertEquals(f1.hashCode(), f2.hashCode());
    }
}
