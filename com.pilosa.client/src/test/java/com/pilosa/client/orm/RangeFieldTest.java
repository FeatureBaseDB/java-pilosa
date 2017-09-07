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
import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class RangeFieldTest {
    @Test
    public void testEqualsFailsWithOtherObject() {
        RangeField field = new RangeField("foo", "int", 10, 20);
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = field.equals("foo");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        RangeField field = new RangeField("foo", "int", -10, 1000);
        assertEquals(field, field);
    }

    @Test
    public void testEquals() {
        RangeField field1 = new RangeField("bar", "int", -10, 1000);
        RangeField field2 = new RangeField("bar", "int", -10, 1000);
        assertTrue(field1.equals(field2));
    }

    @Test
    public void testHashCode() {
        RangeField field1 = new RangeField("foo", "int", -10, 1000);
        RangeField field2 = new RangeField("foo", "int", -10, 1000);
        assertEquals(field1.hashCode(), field2.hashCode());
    }

    @Test(expected = PilosaException.class)
    public void testMaxGreaterThanMin() {
        new RangeField("foo", "int", 10, 9);
    }
}
