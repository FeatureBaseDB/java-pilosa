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

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class ColumnTest {
    @Test
    public void createTest() {
        Column column;

        column = Column.create(1, 100);
        compare(column, 1, 100, "", "", 0);
        column = Column.create(1, 100, 65000);
        compare(column, 1, 100, "", "", 65000);

        column = Column.create(1, "bar");
        compare(column, 1, 0, "bar", "", 0);
        column = Column.create(1, "bar", 65000);
        compare(column, 1, 0, "bar", "", 65000);

        column = Column.create("foo", 100);
        compare(column, 0, 100, "foo", "", 0);
        column = Column.create("foo", 100, 65000);
        compare(column, 0, 100, "foo", "", 65000);

        column = Column.create("foo", "bar");
        compare(column, 0, 0, "foo", "bar", 0);
        column = Column.create("foo", "bar", 65000);
        compare(column, 0, 0, "foo", "bar", 65000);
    }

    @Test
    public void hashCodeTest() {
        Column column1 = Column.create(1, 10, 65000);
        Column column2 = Column.create(1, 10, 65000);
        assertEquals(column1.hashCode(), column2.hashCode());
        assertNotEquals(column1.hashCode(), Column.DEFAULT.hashCode());
    }

    @Test
    public void equalsTest() {
        Column a = Column.create(5, 7, 100000);
        Column b = Column.create(5, 7, 100000);
        assertEquals(a, b);
    }

    @Test
    public void equalsSameObjectTest() {
        Column column = Column.create(5, 7, 100000);
        assertEquals(column, column);
    }

    @Test
    public void notEqualTest() {
        Column column = Column.create(15, 2, 50000);
        assertFalse(column.equals(5));
    }

    @Test
    public void toStringTest() {
        Column column = Column.create(15, 2, 50000);
        assertEquals("15:2 : [50000]", column.toString());
        assertEquals("(default column)", Column.DEFAULT.toString());
    }

    private void compare(Column column, long rowID, long columnID, String rowKey, String columnKey, long timestamp) {
        assertEquals(rowID, column.getRowID());
        assertEquals(columnID, column.getColumnID());
        assertEquals(rowKey, column.getRowKey());
        assertEquals(columnKey, column.getColumnKey());
        assertEquals(timestamp, column.getTimestamp());
    }
}
