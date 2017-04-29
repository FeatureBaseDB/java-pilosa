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

@Category(UnitTest.class)
public class FrameOptionsTest {
    @Test
    public void testBuilder() {
        FrameOptions options = FrameOptions.builder()
                .build();
        compare(options, "rowID", TimeQuantum.NONE, false);

        options = FrameOptions.builder()
                .setRowLabel("the_row_label")
                .build();
        compare(options, "the_row_label", TimeQuantum.NONE, false);

        options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                .build();
        compare(options, "rowID", TimeQuantum.YEAR_MONTH_DAY_HOUR, false);

        options = FrameOptions.builder()
                .setRowLabel("someid")
                .setTimeQuantum(TimeQuantum.YEAR)
                .setInverseEnabled(true)
                .build();
        compare(options, "someid", TimeQuantum.YEAR, true);
    }

    @Test(expected = PilosaException.class)
    public void testInvalidRowLabel() {
        FrameOptions.builder()
                .setRowLabel("#Just an invalid label!")
                .build();
    }

    private void compare(FrameOptions options, String targetRowLabel,
                         TimeQuantum targetTimeQuantum, boolean targetInverseEnabled) {
        assertEquals(targetRowLabel, options.getRowLabel());
        assertEquals(targetTimeQuantum, options.getTimeQuantum());
        assertEquals(targetInverseEnabled, options.isInverseEnabled());
    }
}
