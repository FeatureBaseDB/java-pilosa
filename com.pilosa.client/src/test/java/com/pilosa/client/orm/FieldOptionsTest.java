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
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.exceptions.ValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class FieldOptionsTest {
    @Test
    public void testSetFieldOptions() {
        FieldOptions options;
        String target;

        options = FieldOptions.builder()
                .fieldSet(CacheType.RANKED, 1000)
                .setKeys(true)
                .build();
        compare(options, FieldType.SET, TimeQuantum.NONE, CacheType.RANKED, 1000, 0, 0);
        target = "{\"options\":{\"keys\":true,\"type\":\"set\",\"cacheSize\":1000,\"cacheType\":\"ranked\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("keys", true);
        optionsMap.put("cacheType", "ranked");
        optionsMap.put("cacheSize", 1000);
        options = FieldOptions.fromMap(optionsMap);
        target = "{\"options\":{\"keys\":true,\"type\":\"set\",\"cacheSize\":1000,\"cacheType\":\"ranked\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        options = FieldOptions.builder()
                .fieldSet(CacheType.RANKED)
                .build();
        compare(options, FieldType.SET, TimeQuantum.NONE, CacheType.RANKED, 0, 0, 0);
        target = "{\"options\":{\"type\":\"set\",\"cacheType\":\"ranked\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        options = FieldOptions.builder()
                .fieldSet()
                .build();
        compare(options, FieldType.SET, TimeQuantum.NONE, CacheType.DEFAULT, 0, 0, 0);
        target = "{\"options\":{\"type\":\"set\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));
    }

    @Test
    public void testIntFieldOptions() {
        FieldOptions options = FieldOptions.builder()
                .fieldInt(-100, 500)
                .build();
        compare(options, FieldType.INT, TimeQuantum.NONE, CacheType.DEFAULT, 0, -100, 500);
        String target = "{\"options\":{\"type\":\"int\",\"min\":-100,\"max\":500}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "int");
        optionsMap.put("min", -100);
        optionsMap.put("max", 500);
        options = FieldOptions.fromMap(optionsMap);
        target = "{\"options\":{\"type\":\"int\",\"min\":-100,\"max\":500}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));
    }

    @Test
    public void testTimeFieldOptions() {
        FieldOptions options = FieldOptions.builder()
                .fieldTime(TimeQuantum.MONTH_DAY_HOUR)
                .build();
        compare(options, FieldType.TIME, TimeQuantum.MONTH_DAY_HOUR, CacheType.DEFAULT, 0, 0, 0);
        String target = "{\"options\":{\"type\":\"time\",\"timeQuantum\":\"MDH\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "time");
        optionsMap.put("timeQuantum", "YMDH");
        options = FieldOptions.fromMap(optionsMap);
        target = "{\"options\":{\"type\":\"time\",\"timeQuantum\":\"YMDH\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));
    }

    @Test
    public void testMutexFieldOptions() {
        FieldOptions options;
        String target;

        options = FieldOptions.builder()
                .fieldMutex(CacheType.RANKED, 1000)
                .setKeys(true)
                .build();
        compare(options, FieldType.MUTEX, TimeQuantum.NONE, CacheType.RANKED, 1000, 0, 0);
        target = "{\"options\":{\"keys\":true,\"type\":\"mutex\",\"cacheSize\":1000,\"cacheType\":\"ranked\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        options = FieldOptions.builder()
                .fieldMutex(CacheType.RANKED)
                .build();
        compare(options, FieldType.MUTEX, TimeQuantum.NONE, CacheType.RANKED, 0, 0, 0);
        target = "{\"options\":{\"type\":\"mutex\",\"cacheType\":\"ranked\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        options = FieldOptions.builder()
                .fieldMutex()
                .build();
        compare(options, FieldType.MUTEX, TimeQuantum.NONE, CacheType.DEFAULT, 0, 0, 0);
        target = "{\"options\":{\"type\":\"mutex\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));

        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "mutex");
        optionsMap.put("cacheType", "ranked");
        optionsMap.put("cacheSize", 1000);
        options = FieldOptions.fromMap(optionsMap);
        target = "{\"options\":{\"type\":\"mutex\",\"cacheSize\":1000,\"cacheType\":\"ranked\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));
    }

    @Test
    public void testBoolFieldOptions() {
        FieldOptions options = FieldOptions.builder()
                .fieldBool()
                .build();
        compare(options, FieldType.BOOL, TimeQuantum.NONE, CacheType.DEFAULT, 0, 0, 0);
        String target = "{\"options\":{\"type\":\"bool\"}}";
        assertArrayEquals(stringToSortedChars(target), stringToSortedChars(options.toString()));
    }


    @Test
    public void testKeysOption() {
        FieldOptions options = FieldOptions.builder()
                .setKeys(true)
                .build();
        assertTrue(options.isKeys());
    }

    @Test(expected = PilosaException.class)
    public void testFieldOptionsJsonProcessingException() {
        FieldOptions options = FieldOptions.builder().build();
        options.setExtra(new SerializeThis());
        String s = options.toString();
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
        FieldOptions options1, options2;

        options1 = FieldOptions.builder()
                .fieldSet(CacheType.RANKED, 100)
                .build();
        options2 = FieldOptions.builder()
                .fieldSet(CacheType.RANKED, 100)
                .build();
        assertEquals(options1.hashCode(), options2.hashCode());

        options1 = FieldOptions.builder()
                .fieldInt(-100, 200)
                .build();
        options2 = FieldOptions.builder()
                .fieldInt(-100, 200)
                .build();
        assertEquals(options1.hashCode(), options2.hashCode());

        options1 = FieldOptions.builder()
                .fieldTime(TimeQuantum.YEAR_MONTH)
                .build();
        options2 = FieldOptions.builder()
                .fieldTime(TimeQuantum.YEAR_MONTH)
                .build();
        assertEquals(options1.hashCode(), options2.hashCode());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheTypeRequiresSetField() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "int");
        optionsMap.put("cacheType", "ranked");
        FieldOptions options = FieldOptions.fromMap(optionsMap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheSizeRequiresSetField() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "int");
        optionsMap.put("cacheSize", 1000);
        FieldOptions options = FieldOptions.fromMap(optionsMap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeQuantumRequiresTimeField() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "set");
        optionsMap.put("timeQuantum", "YMDH");
        FieldOptions options = FieldOptions.fromMap(optionsMap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMinRequiresIntField() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "set");
        optionsMap.put("min", 1000);
        FieldOptions options = FieldOptions.fromMap(optionsMap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxRequiresIntField() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "set");
        optionsMap.put("max", 1000);
        FieldOptions options = FieldOptions.fromMap(optionsMap);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidType() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("type", "foo");
        FieldOptions options = FieldOptions.fromMap(optionsMap);
    }

    private void compare(FieldOptions options,
                         FieldType targetFieldType,
                         TimeQuantum targetTimeQuantum,
                         CacheType targetCacheType,
                         int targetCacheSize,
                         int targetMin,
                         int targetMax) {
        assertEquals(targetFieldType, options.getFieldType());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
        assertEquals(targetCacheType, options.getCacheType());
        assertEquals(targetCacheSize, options.getCacheSize());
        assertEquals(targetMin, options.getMin());
        assertEquals(targetMax, options.getMax());
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

class SerializeThis {
}