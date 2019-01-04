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

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class IntResult implements QueryResult {
    @Override
    public int getType() {
        return QueryResultType.INT;
    }

    @Override
    public RowResult getRow() {
        return RowResult.defaultResult();
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
        return 0;
    }

    @Override
    public boolean isChanged() {
        return false;
    }

    @Override
    public List<GroupCount> getGroupCounts() {
        return GroupCountsResult.defaultItems();
    }

    @Override
    public RowIdentifiersResult getRowIdentifiers() {
        return RowIdentifiersResult.defaultResult();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof IntResult)) {
            return false;
        }
        return this.count == ((IntResult) obj).count;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.count)
                .toHashCode();
    }

    static IntResult create(long count) {
        IntResult result = new IntResult();
        result.count = count;
        return result;
    }

    static IntResult fromInternal(Internal.QueryResult q) {
        return IntResult.create(q.getN());
    }

    private long count;
}
