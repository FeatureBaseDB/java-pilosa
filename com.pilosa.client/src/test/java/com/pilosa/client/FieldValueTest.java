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

import edu.emory.mathcs.backport.java.util.Collections;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class FieldValueTest {
    @Test
    public void createTest() {
        FieldValue fieldValue;

        fieldValue = FieldValue.create(1, 100);
        compare(fieldValue, 1, "", 100);

        fieldValue = FieldValue.create("one", 100);
        compare(fieldValue, 0, "one", 100);
    }

    @Test
    public void hashCodeTest() {
        FieldValue a = FieldValue.create(1, -100);
        FieldValue b = FieldValue.create(1, -100);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a.hashCode(), FieldValue.DEFAULT.hashCode());
    }

    @Test
    public void equalsTest() {
        FieldValue a = FieldValue.create(1, 100000);
        FieldValue b = FieldValue.create(1, 100000);
        assertEquals(a, b);

        FieldValue c = FieldValue.create("foo", 100);
        FieldValue d = FieldValue.create("foo", 100);
        assertEquals(c, d);
        assertNotEquals(a, c);
    }

    @Test
    public void equalsSameObjectTest() {
        FieldValue a = FieldValue.create(1, 100000);
        assertEquals(a, a);
    }

    @Test
    public void notEqualTest() {
        FieldValue a = FieldValue.create(15, 50000);
        assertFalse(a.equals(5));
    }

    @Test
    public void toStringTest() {
        FieldValue a = FieldValue.create(15, 50000);
        assertEquals("15  = 50000", a.toString());
        assertEquals("(default field value)", FieldValue.DEFAULT.toString());
    }


    @Test
    public void compareToTest() {
        FieldValue v1 = FieldValue.create(10, 5);
        FieldValue v2 = FieldValue.create(5, 7);
        FieldValue v3 = FieldValue.create(5, 3);
        List<FieldValue> fieldValueList = Arrays.asList(v1, v2, v3);
        Collections.sort(fieldValueList);
        assertEquals(fieldValueList.get(0), v2);
        assertEquals(fieldValueList.get(1), v3);
        assertEquals(fieldValueList.get(2), v1);
    }

    private void compare(FieldValue fieldValue, long columnID, String columnKey, long value) {
        assertEquals(columnID, fieldValue.columnID);
        assertEquals(columnKey, fieldValue.columnKey);
        assertEquals(value, fieldValue.value);
    }
}
