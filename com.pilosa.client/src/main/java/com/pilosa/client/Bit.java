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

public class Bit {
    private Internal.Bit iBit = null;

    private Bit() {
    }

    public static Bit create(long rowID, long columnID) {
        Bit bit = new Bit();
        bit.iBit = Internal.Bit.newBuilder()
                .setRowID(rowID)
                .setColumnID(columnID)
                .build();
        return bit;
    }

    public static Bit create(long rowID, long columnID, long timestamp) {
        Bit bit = new Bit();
        bit.iBit = Internal.Bit.newBuilder()
                .setRowID(rowID)
                .setColumnID(columnID)
                .setTimestamp(timestamp)
                .build();
        return bit;
    }

    @SuppressWarnings("WeakerAccess")
    public long getRowID() {
        return this.iBit.getRowID();
    }

    @SuppressWarnings("WeakerAccess")
    public long getColumnID() {
        return this.iBit.getColumnID();
    }

    @SuppressWarnings("WeakerAccess")
    public long getTimestamp() {
        return this.iBit.getTimestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Bit)) {
            return false;
        }

        Bit bit = (Bit) o;
        return this.iBit.equals(bit.iBit);
    }

    @Override
    public int hashCode() {
        return this.iBit.hashCode();
    }
}
