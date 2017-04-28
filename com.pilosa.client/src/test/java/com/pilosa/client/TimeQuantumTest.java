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

import com.pilosa.client.exceptions.ValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class TimeQuantumTest {
    @Test
    public void testGetStringValue() {
        Map<TimeQuantum, String> vs = getTargetMap();
        for (Map.Entry<TimeQuantum, String> e : vs.entrySet()) {
            assertEquals(e.getValue(), e.getKey().getStringValue());
        }
    }

    @Test
    public void testFromString() {
        Map<TimeQuantum, String> vs = getTargetMap();
        for (Map.Entry<TimeQuantum, String> e : vs.entrySet()) {
            assertEquals(TimeQuantum.fromString(e.getValue()), e.getKey());
        }
    }

    @Test(expected = ValidationException.class)
    public void testFromStringInvalidString() {
        TimeQuantum.fromString("INV");
    }

    private Map<TimeQuantum, String> getTargetMap() {
        Map<TimeQuantum, String> vs = new HashMap<>();
        vs.put(TimeQuantum.YEAR_MONTH_DAY_HOUR, "YMDH");
        vs.put(TimeQuantum.YEAR_MONTH_DAY, "YMD");
        vs.put(TimeQuantum.YEAR_MONTH, "YM");
        vs.put(TimeQuantum.YEAR, "Y");
        vs.put(TimeQuantum.MONTH_DAY_HOUR, "MDH");
        vs.put(TimeQuantum.MONTH_DAY, "MD");
        vs.put(TimeQuantum.MONTH, "M");
        vs.put(TimeQuantum.DAY, "D");
        vs.put(TimeQuantum.DAY_HOUR, "DH");
        vs.put(TimeQuantum.HOUR, "H");
        vs.put(TimeQuantum.NONE, "");
        return vs;
    }
}
