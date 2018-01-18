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
public class FrameOptionsTest {
    @Test
    public void testBuilder() {
        FrameOptions options = FrameOptions.builder()
                .build();
        compare(options, TimeQuantum.NONE, false, CacheType.DEFAULT, 0);

        options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, TimeQuantum.YEAR_MONTH_DAY_HOUR, false, CacheType.DEFAULT, 0);

        options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR)
                .setInverseEnabled(true)
                .build();
        compare(options, TimeQuantum.YEAR, true, CacheType.DEFAULT, 0);

        options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR)
                .setInverseEnabled(true)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(10000)
                .build();
        compare(options, TimeQuantum.YEAR, true, CacheType.RANKED, 10000);
    }

    @Test
    public void testFrameOptionsToString() {
        FrameOptions options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.DAY_HOUR)
                .setInverseEnabled(true)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(1000)
                .addIntField("foo", 10, 100)
                .addIntField("bar", -1, 1)
                .build();
        String target = "{\"options\": {\"rowLabel\":\"rowID\",\"inverseEnabled\":true,\"timeQuantum\":\"DH\",\"cacheType\":\"ranked\",\"cacheSize\":1000,\"rangeEnabled\":true,\"fields\":[{\"name\":\"bar\",\"min\":-1,\"type\":\"int\",\"max\":1},{\"name\":\"foo\",\"min\":10,\"type\":\"int\",\"max\":100}]}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        FrameOptions options = FrameOptions.builder().build();
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = options.equals("foo");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        FrameOptions options = FrameOptions.builder().build();
        assertEquals(options, options);
    }

    @Test
    public void testHashCode() {
        FrameOptions options1 = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
                .setInverseEnabled(true)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(1000)
                .addIntField("foo", 10, 1000)
                .build();
        FrameOptions options2 = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
                .setInverseEnabled(true)
                .setCacheType(CacheType.RANKED)
                .setCacheSize(1000)
                .addIntField("foo", 10, 1000)
                .build();
        assertEquals(options1.hashCode(), options2.hashCode());
    }

    @Test
    public void testIsRangeEnabled() {
        FrameOptions options = FrameOptions.withDefaults();
        assertFalse(options.isRangeEnabled());
        options = FrameOptions.builder()
                .addIntField("baz", 10, 100)
                .build();
        assertTrue(options.isRangeEnabled());
    }

    private void compare(FrameOptions options,
                         TimeQuantum targetTimeQuantum, boolean targetInverseEnabled,
                         CacheType targetCacheType, int targetCacheSize) {
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
        assertEquals(targetInverseEnabled, options.isInverseEnabled());
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
