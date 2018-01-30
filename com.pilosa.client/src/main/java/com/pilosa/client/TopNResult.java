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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class TopNResult implements QueryResult {
    @Override
    public int getType() {
        return QueryResultType.PAIRS;
    }

    @Override
    public BitmapResult getBitmap() {
        return BitmapResult.defaultResult();
    }

    @Override
    public CountResultItem[] getCountItems() {
        return this.items;
    }

    @Override
    public long getCount() {
        return 0;
    }

    @Override
    public long getSum() {
        return 0;
    }

    @Override
    public boolean isChanged() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof TopNResult)) {
            return false;
        }
        TopNResult rhs = (TopNResult) obj;
        return new EqualsBuilder()
                .append(this.items, rhs.items)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.items)
                .toHashCode();
    }

    static TopNResult create(CountResultItem[] items) {
        TopNResult result = new TopNResult();
        result.items = items;
        return result;
    }

    static TopNResult fromInternal(Internal.QueryResult q) {
        List<Internal.Pair> listItems = q.getPairsList();
        final int itemCount = listItems.size();
        CountResultItem[] items = new CountResultItem[itemCount];
        for (int i = 0; i < itemCount; i++) {
            items[i] = CountResultItem.fromInternal(listItems.get(i));
        }
        return TopNResult.create(items);
    }

    static CountResultItem[] defaultItems() {
        return defaultItems;
    }

    private static CountResultItem[] defaultItems = new CountResultItem[0];
    private CountResultItem[] items;
}
