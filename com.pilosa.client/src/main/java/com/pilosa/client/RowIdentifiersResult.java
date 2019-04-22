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

public final class RowIdentifiersResult implements QueryResult {
    public static RowIdentifiersResult withIDs(List<Long> rowIDs) {
        return new RowIdentifiersResult(rowIDs, null);
    }

    public static RowIdentifiersResult withKeys(List<String> rowKeys) {
        return new RowIdentifiersResult(null, rowKeys);
    }

    @Override
    public int getType() {
        return QueryResultType.ROW_IDENTIFIERS;
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
        return 0;
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
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof RowIdentifiersResult)) {
            return false;
        }
        RowIdentifiersResult rhs = (RowIdentifiersResult) obj;
        return new EqualsBuilder()
                .append(this.rowIDs, rhs.rowIDs)
                .append(this.rowKeys, rhs.rowKeys)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.rowIDs)
                .append(this.rowKeys)
                .toHashCode();
    }

    static RowIdentifiersResult defaultResult() {
        return defaultResult;
    }

    static RowIdentifiersResult fromInternal(Internal.QueryResult q) {
        Internal.RowIdentifiers rowIdentifiers = q.getRowIdentifiers();
        return new RowIdentifiersResult(rowIdentifiers.getRowsList(), rowIdentifiers.getKeysList());
    }

    static {
        defaultResult = new RowIdentifiersResult(null, null);
        ;
    }

    private RowIdentifiersResult(List<Long> rowIDs, List<String> rowKeys) {
        this.rowIDs = (rowIDs == null) ? new ArrayList<Long>(0) : rowIDs;
        this.rowKeys = (rowKeys == null) ? new ArrayList<String>(0) : rowKeys;
    }

    private final static RowIdentifiersResult defaultResult;

    private final List<Long> rowIDs;
    private final List<String> rowKeys;
}
