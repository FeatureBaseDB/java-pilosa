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

public class ValueCountResult implements QueryResult {
    @Override
    public int getType() {
        return QueryResultType.VAL_COUNT;
    }

    @Override
    public BitmapResult getBitmap() {
        return BitmapResult.defaultResult();
    }

    @Override
    public List<CountResultItem> getCountItems() {
        return TopNResult.defaultItems();
    }

    @Override
    public long getCount() {
        return this.count;
    }

    @Override
    public long getValue() {
        return this.value;
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
        if (!(obj instanceof ValueCountResult)) {
            return false;
        }
        ValueCountResult rhs = (ValueCountResult) obj;
        return new EqualsBuilder()
                .append(this.value, rhs.value)
                .append(this.count, rhs.count)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.value)
                .append(this.count)
                .toHashCode();
    }

    static ValueCountResult create(long sum, long count) {
        ValueCountResult result = new ValueCountResult();
        result.value = sum;
        result.count = count;
        return result;
    }

    static ValueCountResult fromInternal(Internal.QueryResult q) {
        Internal.ValCount obj = q.getValCount();
        return create(obj.getVal(), obj.getCount());
    }

    private long value;
    private long count;
}
