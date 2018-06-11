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

import com.pilosa.client.orm.Field;
import com.pilosa.client.orm.Index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

class SliceBits {
    public static SliceBits create(final Field field, final long slice) {
        return new SliceBits(field, slice);
    }

    public Index getIndex() {
        return this.field.getIndex();
    }

    public long getSlice() {
        return this.slice;
    }

    public List<Bit> getBits() {
        if (!this.sorted) {
            Collections.sort(bits, bitComparator);
            this.sorted = true;
        }
        return this.bits;
    }

    public void add(Bit bit) {
        this.bits.add(bit);
    }

    public void clear() {
        this.bits.clear();
    }

    Internal.ImportRequest convertToImportRequest() {
        List<Long> bitmapIDs = new ArrayList<>(bits.size());
        List<Long> columnIDs = new ArrayList<>(bits.size());
        List<Long> timestamps = new ArrayList<>(bits.size());
        for (Bit bit : bits) {
            bitmapIDs.add(bit.getRowID());
            columnIDs.add(bit.getColumnID());
            timestamps.add(bit.getTimestamp());
        }
        return Internal.ImportRequest.newBuilder()
                .setIndex(this.field.getIndex().getName())
                .setFrame(this.field.getName())
                .setSlice(slice)
                .addAllRowIDs(bitmapIDs)
                .addAllColumnIDs(columnIDs)
                .addAllTimestamps(timestamps)
                .build();
    }

    SliceBits(final Field field, final long slice) {
        this.field = field;
        this.slice = slice;
        this.bits = new ArrayList<>();
    }

    private static Comparator<Bit> bitComparator = new BitComparator();
    private final Field field;
    private final long slice;
    private List<Bit> bits;
    private boolean sorted = false;
}
