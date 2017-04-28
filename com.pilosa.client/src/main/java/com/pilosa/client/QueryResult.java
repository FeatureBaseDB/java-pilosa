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

import java.util.ArrayList;
import java.util.List;

/**
 * Represent one of the results in the response.
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public final class QueryResult {
    private BitmapResult bitmapResult = null;
    private List<CountResultItem> countItems = null;
    private long count = 0L;

    QueryResult() {
    }

    QueryResult(BitmapResult bitmapResult, List<CountResultItem> countItems, long count) {
        this.bitmapResult = bitmapResult;
        this.countItems = countItems;
        this.count = count;
    }

    static QueryResult fromInternal(Internal.QueryResult q) {
        List<CountResultItem> items = new ArrayList<>(q.getPairsCount());
        for (Internal.Pair pair : q.getPairsList()) {
            items.add(CountResultItem.fromInternal(pair));
        }
        BitmapResult bitmapResult = null;
        if (q.hasBitmap()) {
            bitmapResult = BitmapResult.fromInternal(q.getBitmap());
        }
        return new QueryResult(bitmapResult, items, q.getN());
    }

    /**
     * Returns the bitmap result.
     *
     * @return bitmap result if it exists or <code>null</code>
     */
    public BitmapResult getBitmap() {
        return this.bitmapResult;
    }

    /**
     * Returns the count result items (from TopN query).
     *
     * @return count result items
     */
    public List<CountResultItem> getCountItems() {
        return this.countItems;
    }

    /**
     * Returns the result from Count query.
     *
     * @return number of columns for the bitmap in the query
     */
    public long getCount() {
        return this.count;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof QueryResult)) {
            return false;
        }
        QueryResult rhs = (QueryResult) obj;
        return new EqualsBuilder()
                .append(bitmapResult, rhs.bitmapResult)
                .append(countItems, rhs.countItems)
                .append(count, rhs.count)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(bitmapResult)
                .append(countItems)
                .append(count)
                .toHashCode();
    }
}
