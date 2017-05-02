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

/**
 * Valid time quantum values for frames having support for that.
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/">Data Model</a>
 */
public enum TimeQuantum {
    NONE(0),
    YEAR(TimeQuantum.Y),
    MONTH(TimeQuantum.M),
    DAY(TimeQuantum.D),
    HOUR(TimeQuantum.H),
    YEAR_MONTH(TimeQuantum.Y | TimeQuantum.M),
    MONTH_DAY(TimeQuantum.M | TimeQuantum.D),
    DAY_HOUR(TimeQuantum.D | TimeQuantum.H),
    YEAR_MONTH_DAY(TimeQuantum.Y | TimeQuantum.M | TimeQuantum.D),
    MONTH_DAY_HOUR(TimeQuantum.M | TimeQuantum.D | TimeQuantum.H),
    YEAR_MONTH_DAY_HOUR(TimeQuantum.Y | TimeQuantum.M | TimeQuantum.D | TimeQuantum.H);

    /**
     * Converts a string to the corresponding TimeQuantum.
     *
     * @param s the string to be converted
     * @return a TimeQuantum object
     */
    public static TimeQuantum fromString(String s) {
        switch (s) {
            case "":
                return TimeQuantum.NONE;
            case "Y":
                return TimeQuantum.YEAR;
            case "M":
                return TimeQuantum.MONTH;
            case "D":
                return TimeQuantum.DAY;
            case "H":
                return TimeQuantum.HOUR;
            case "YM":
                return TimeQuantum.YEAR_MONTH;
            case "MD":
                return TimeQuantum.MONTH_DAY;
            case "DH":
                return TimeQuantum.DAY_HOUR;
            case "YMD":
                return TimeQuantum.YEAR_MONTH_DAY;
            case "MDH":
                return TimeQuantum.MONTH_DAY_HOUR;
            case "YMDH":
                return TimeQuantum.YEAR_MONTH_DAY_HOUR;
        }
        throw new ValidationException(String.format("Invalid time quantum string: %s", s));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(4);
        if ((this.value & Y) == Y) sb.append('Y');
        if ((this.value & M) == M) sb.append('M');
        if ((this.value & D) == D) sb.append('D');
        if ((this.value & H) == H) sb.append('H');
        return sb.toString();
    }

    TimeQuantum(int value) {
        this.value = value;
    }

    private final int value;
    private static final byte Y = 0b00000001;
    private static final byte M = 0b00000010;
    private static final byte D = 0b00000100;
    private static final byte H = 0b00001000;
}
