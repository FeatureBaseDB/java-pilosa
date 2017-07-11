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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(UnitTest.class)
public class IndexOptionsTest {
    @Test
    public void testBuilder() {
        IndexOptions options = IndexOptions.builder()
                .build();
        compare(options, "columnID", TimeQuantum.NONE);

        options = IndexOptions.builder()
                .setColumnLabel("random_lbl")
                .build();
        compare(options, "random_lbl", TimeQuantum.NONE);

        options = IndexOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, "columnID", TimeQuantum.YEAR_MONTH_DAY_HOUR);

        options = IndexOptions.builder()
                .setColumnLabel("some_label")
                .setTimeQuantum(TimeQuantum.DAY)
                .build();
        compare(options, "some_label", TimeQuantum.DAY);
    }

    @Test(expected = PilosaException.class)
    public void testInvalidColumnLabel() {
        IndexOptions.builder()
                .setColumnLabel("#Justa an invalid label!")
                .build();
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
    public void testHashCode() {
        IndexOptions options1 = IndexOptions.builder()
                .setColumnLabel("col")
                .setTimeQuantum(TimeQuantum.YEAR_MONTH)
                .build();
        IndexOptions options2 = IndexOptions.builder()
                .setColumnLabel("col")
                .setTimeQuantum(TimeQuantum.YEAR_MONTH)
                .build();
        assertEquals(options1.hashCode(), options2.hashCode());
    }

    @Test
    public void testIndexOptionsToString() {
        IndexOptions options = IndexOptions.builder()
                .setColumnLabel("COLID")
                .setTimeQuantum(TimeQuantum.YEAR_MONTH)
                .build();
        String target = "{\"options\":{\"columnLabel\":\"COLID\", \"timeQuantum\":\"YM\"}}";
        assertEquals(target, options.toString());
    }

    private void compare(IndexOptions options, String targetColumnLabel, TimeQuantum targetTimeQuantum) {
        assertEquals(targetColumnLabel, options.getColumnLabel());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
    }
}
