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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class IndexOptionsTest {
    @Test
    public void testOptions() {
        IndexOptions options = IndexOptions.builder().build();
        assertFalse(options.isKeys());
        options = IndexOptions.builder()
                .setKeys(true)
                .setTrackExistence(true)
                .build();
        assertTrue(options.isKeys());
        assertTrue(options.isTrackExistence());

        // deprecated methods
        options = IndexOptions.builder()
                .keys(true)
                .trackExistence(true)
                .build();
        assertTrue(options.isKeys());
        assertTrue(options.isTrackExistence());
    }

    @Test
    public void testFromMap() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("keys", true);
        optionsMap.put("trackExistence", false);
        IndexOptions options = IndexOptions.fromMap(optionsMap);
        assertTrue(options.isKeys());
        assertFalse(options.isTrackExistence());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromMapInvalidKey() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("KEYS", true);
        IndexOptions options = IndexOptions.fromMap(optionsMap);
    }

    @Test(expected = ClassCastException.class)
    public void testFromMapInvalidKeysValue() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("keys", 1);
        IndexOptions options = IndexOptions.fromMap(optionsMap);
    }

    @Test(expected = ClassCastException.class)
    public void testFromMapInvalidTrackExistenceValue() {
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("trackExistence", 1);
        IndexOptions options = IndexOptions.fromMap(optionsMap);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        IndexOptions options = IndexOptions.builder().build();
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = options.equals("foo");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        IndexOptions options = IndexOptions.builder().build();
        assertEquals(options, options);
    }

    @Test
    public void testEquals() {
        IndexOptions options1, options2;

        options1 = IndexOptions.builder()
                .setKeys(true)
                .build();
        options2 = IndexOptions.builder()
                .setKeys(true)
                .build();
        assertEquals(options1, options2);
    }

    @Test
    public void testHashCode() {
        IndexOptions options1, options2;

        options1 = IndexOptions.builder()
                .setKeys(true)
                .build();
        options2 = IndexOptions.builder()
                .setKeys(true)
                .build();
        assertEquals(options1.hashCode(), options2.hashCode());
    }
}
