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
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class TopNResultTest {
    @Test
    public void testCreateTopNResult() {
        TopNResult result = createSampleResult();
        List<CountResultItem> items = result.getCountItems();
        assertEquals(1, items.size());
        assertEquals(QueryResultType.PAIRS, result.getType());
        List<CountResultItem> targetItems = new ArrayList<>();
        targetItems.add(CountResultItem.create(5, "", 10));
        assertEquals(BitmapResult.defaultResult(), result.getBitmap());
        assertEquals(targetItems, result.getCountItems());
        assertEquals(0L, result.getCount());
        assertEquals(0L, result.getSum());
        assertEquals(false, result.isChanged());
    }

    @Test
    public void testEquals() {
        TopNResult result1 = createSampleResult();
        TopNResult result2 = createSampleResult();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (new TopNResult()).equals(0);
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        TopNResult result = createSampleResult();
        assertEquals(result, result);
    }

    @Test
    public void testHashCode() {
        TopNResult result1 = createSampleResult();
        TopNResult result2 = createSampleResult();
        assertEquals(result1.hashCode(), result2.hashCode());
    }

    private TopNResult createSampleResult() {
        List<CountResultItem> items = new ArrayList<>();
        items.add(CountResultItem.create(5, "", 10));
        return TopNResult.create(items);
    }
}
