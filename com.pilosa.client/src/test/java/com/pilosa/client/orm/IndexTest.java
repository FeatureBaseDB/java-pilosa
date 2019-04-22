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
import com.pilosa.client.exceptions.ValidationException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(UnitTest.class)
public class IndexTest {
    @Before
    public void setUp() {
        this.schema = Schema.defaultSchema();
    }

    @Test(expected = ValidationException.class)
    public void checkValidatorWasCalledTest() {
        Index.create("a:b");
    }

    @Test
    public void checkUnionArgumentCountEnforced()
            throws NoSuchMethodException, IllegalAccessException {
        assertEquals(false, checkArguments("union", 0));
        assertEquals(false, checkArguments("union", 1));
        assertEquals(false, checkArguments("union", 2));
    }

    @Test
    public void checkIntersectArgumentCountEnforced()
            throws NoSuchMethodException, IllegalAccessException {
        assertEquals(true, checkArguments("intersect", 0));
        assertEquals(false, checkArguments("intersect", 1));
        assertEquals(false, checkArguments("intersect", 2));
    }

    @Test
    public void checkDifferenceArgumentCountEnforced()
            throws NoSuchMethodException, IllegalAccessException {
        assertEquals(true, checkArguments("difference", 0));
        assertEquals(false, checkArguments("difference", 1));
        assertEquals(false, checkArguments("difference", 2));
    }

    @Test
    public void checkXorArgumentCountEnforced()
            throws NoSuchMethodException, IllegalAccessException {
        assertEquals(true, checkArguments("xor", 0));
        assertEquals(true, checkArguments("xor", 1));
        assertEquals(false, checkArguments("xor", 2));
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = this.schema.index("foo").equals("foo");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        Index index = this.schema.index("some-index");
        assertEquals(index, index);
    }

    @Test
    public void testHashCode() {
        Index index1 = this.schema.index("foo");
        Index index2 = Index.create("foo");
        assertEquals(index1.hashCode(), index2.hashCode());
    }


    @Test
    public void testFromMap() {
        Index index = this.schema.index("some-index");
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "mutex");
        optionsMap.put("cacheType", "ranked");
        optionsMap.put("cacheSize", 1000);
        Field field1 = index.field("some-field", optionsMap);
        FieldOptions options = FieldOptions.builder()
                .fieldMutex(CacheType.RANKED, 1000)
                .build();
        Field field2 = index.field("some-field", options);
        assertEquals(field1, field2);
    }

    private boolean checkArguments(String methodName, int count)
            throws NoSuchMethodException, IllegalAccessException {
        Index index = Index.create("my-index");
        Field field = index.field("my-field");
        Method m = index.getClass().getMethod(methodName, PqlRowQuery[].class);
        PqlRowQuery queries[] = new PqlRowQuery[count];
        for (int i = 0; i < count; i++) {
            queries[i] = field.row(i);
        }
        try {
            m.invoke(index, (Object) queries);
        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause();
            if (cause == null) {
                return false;
            }
            if (cause.getClass().equals(IllegalArgumentException.class)) {
                return true;
            }
        }
        return false;
    }

    private Schema schema;
}