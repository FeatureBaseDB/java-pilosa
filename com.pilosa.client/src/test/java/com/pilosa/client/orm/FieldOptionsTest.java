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

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class FieldOptionsTest {
    @Test
    public void testBuilder() {
        FieldOptions options = FieldOptions.builder()
                .build();
        compare(options, TimeQuantum.NONE, false, CacheType.DEFAULT, 0);

        options = FieldOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, TimeQuantum.YEAR_MONTH_DAY_HOUR, false, CacheType.DEFAULT, 0);

        options = FieldOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR)
                .build();
        compare(options, TimeQuantum.YEAR, true, CacheType.DEFAULT, 0);

        options = FieldOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(10000)
                .build();
        compare(options, TimeQuantum.YEAR, true, CacheType.RANKED, 10000);
    }

    @Test
    public void testFrameOptionsToString() {
        FieldOptions options = FieldOptions.builder()
                .setTimeQuantum(TimeQuantum.DAY_HOUR)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(1000)
                .fieldInt(-10, 100)
                .build();
        String target = "{\"options\":{\"cacheSize\":1000,\"min\":-10,\"max\":100,\"type\":\"int\",\"cacheType\":\"ranked\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        FieldOptions options = FieldOptions.builder().build();
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = options.equals("foo");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        FieldOptions options = FieldOptions.builder().build();
        assertEquals(options, options);
    }

    @Test
    public void testHashCode() {
        FieldOptions options1 = FieldOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(1000)
                .fieldInt(10, 1000)
                .build();
        FieldOptions options2 = FieldOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(1000)
                .fieldInt(10, 1000)
                .build();
        assertEquals(options1.hashCode(), options2.hashCode());
    }

    private void compare(FieldOptions options,
                         TimeQuantum targetTimeQuantum, boolean targetInverseEnabled,
                         CacheType targetCacheType, int targetCacheSize) {
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
        assertEquals(targetCacheType, options.getCacheType());
        assertEquals(targetCacheSize, options.getCacheSize());
    }

    private Object[] stringToSortedChars(String s) {
        List<Character> characterList = new ArrayList<>();
        for (char c : s.toCharArray()) {
            characterList.add(c);
        }
        Object[] arr = characterList.toArray();
        Arrays.sort(arr);
        return arr;
    }
}