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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class RowResultTest {
    @Test
    public void testCreateRowResult() {
        RowResult result = createSampleResult();
        Map<String, Object> attrs = result.getAttributes();
        assertEquals(1, attrs.size());
        assertEquals("blue", attrs.get("color"));
        assertEquals(0, result.getKeys().size());
        List<Long> bits = result.getBits();
        assertEquals(2, bits.size());
        assertEquals(42, (long) bits.get(0));
        assertEquals(45, (long) bits.get(1));
        assertEquals(QueryResultType.ROW, result.getType());
        assertEquals(TopNResult.defaultItems(), result.getCountItems());
        assertEquals(0L, result.getCount());
        assertEquals(0L, result.getValue());
        assertEquals(false, result.isChanged());
    }

    @Test
    public void testCreateRowResultEnterprise() {
        RowResult result = createSampleEnterpriseResult();
        Map<String, Object> attrs = result.getAttributes();
        assertEquals(1, attrs.size());
        assertEquals("blue", attrs.get("color"));
        assertEquals(0, result.getBits().size());
        List<String> keys = result.getKeys();
        assertEquals(2, keys.size());
        assertEquals("2a84a392-529e-4603-ab25-fe2ceea3167e", keys.get(0));
        assertEquals("ad76b92c-2fd0-472a-8b7f-ef6daf5a3305", keys.get(1));
        assertEquals(QueryResultType.ROW, result.getType());
        assertEquals(TopNResult.defaultItems(), result.getCountItems());
        assertEquals(0L, result.getCount());
        assertEquals(0L, result.getValue());
        assertEquals(false, result.isChanged());
    }

    @Test
    public void testRowResultToString() {
        RowResult result = createSampleResult();
        String s = result.toString();
        assertEquals("RowResult(attrs={color=blue}, bits=[42, 45], keys=[])", s);
    }

    @Test
    public void testEquals() {
        RowResult result1 = createSampleResult();
        RowResult result2 = createSampleResult();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = RowResult.defaultResult().equals(0);
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        RowResult result = createSampleResult();
        assertEquals(result, result);
    }

    @Test
    public void testHashCode() {
        RowResult result1 = createSampleResult();
        RowResult result2 = createSampleResult();
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    private RowResult createSampleResult() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("color", "blue");
        List<Long> bits = new ArrayList<>(2);
        bits.add(42L);
        bits.add(45L);
        return RowResult.create(attrs, bits, null);
    }

    private RowResult createSampleEnterpriseResult() {
        Map<String, Object> attrs = new HashMap<>(1);
        attrs.put("color", "blue");
        List<String> keys = new ArrayList<>(2);
        keys.add("2a84a392-529e-4603-ab25-fe2ceea3167e");
        keys.add("ad76b92c-2fd0-472a-8b7f-ef6daf5a3305");
        return RowResult.create(attrs, null, keys);
    }
}
