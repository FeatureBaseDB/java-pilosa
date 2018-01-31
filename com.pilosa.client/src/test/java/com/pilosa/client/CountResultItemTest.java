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

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class CountResultItemTest {
    @Test
    public void testCreateCountResult() {
        CountResultItem result = createSampleResult();
        assertEquals(45, result.getID());
        assertEquals("", result.getKey());
        assertEquals(12, result.getCount());
    }

    @Test
    public void testCreateCountResultEnterprise() {
        CountResultItem result = CountResultItem.create(55, "a5c3c775-d7a9-4c1c-9adc-971e4fb9d04e", 100);
        assertEquals(0, result.getID());
        assertEquals("a5c3c775-d7a9-4c1c-9adc-971e4fb9d04e", result.getKey());
        assertEquals(100, result.getCount());
    }

    @Test
    public void testCreateCountResultDefaultConstructor() {
        new CountResultItem();
    }


    @Test
    public void testCountResultToString() {
        CountResultItem result = createSampleResult();
        assertEquals("CountResultItem(id=45, count=12)", result.toString());

        result = CountResultItem.create(0, "foo", 100);
        assertEquals("CountResultItem(key=\"foo\", count=100)", result.toString());
    }

    @Test
    public void testEquals() {
        CountResultItem result1 = createSampleResult();
        CountResultItem result2 = createSampleResult();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = CountResultItem.create(1, "", 2).equals(0);
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        CountResultItem result = createSampleResult();
        assertEquals(result, result);
    }

    @Test
    public void testHashCode() {
        CountResultItem result1 = createSampleResult();
        CountResultItem result2 = createSampleResult();
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    private CountResultItem createSampleResult() {
        return CountResultItem.create(45, "", 12);
    }
}
