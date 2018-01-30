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
import static org.junit.Assert.*;

@Category(UnitTest.class)
public class NullResultTest {
    @Test
    public void testCreateNullResult() {
        NullResult result = NullResult.defaultResult();
        assertEquals(QueryResultType.NIL, result.getType());
        assertEquals(BitmapResult.defaultResult(), result.getBitmap());
        assertArrayEquals(TopNResult.defaultItems(), result.getCountItems());
        assertEquals(0L, result.getCount());
        assertEquals(0L, result.getSum());
        assertEquals(false, result.isChanged());
    }

    @Test
    public void testEquals() {
        NullResult result1 = NullResult.defaultResult();
        NullResult result2 = NullResult.defaultResult();
        boolean e = result1.equals(result2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (NullResult.defaultResult()).equals(0);
        assertFalse(e);
    }

    @Test
    public void testHashCode() {
        NullResult result1 = NullResult.defaultResult();
        NullResult result2 = NullResult.defaultResult();
        assertEquals(result1.hashCode(), result2.hashCode());
    }
}
