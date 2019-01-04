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

import java.util.ArrayList;
import java.util.List;

public final class GroupCountsResult implements QueryResult {
    @Override
    public int getType() {
        return QueryResultType.GROUP_COUNTS;
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
        return this.items;
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
        if (!(obj instanceof GroupCountsResult)) {
            return false;
        }
        GroupCountsResult rhs = (GroupCountsResult) obj;
        return this.items.equals(rhs.items);
    }

    @Override
    public int hashCode() {
        return this.items.hashCode();
    }

    static GroupCountsResult fromInternal(Internal.QueryResult q) {
        List<GroupCount> items = new ArrayList<>(q.getGroupCountsCount());
        for (Internal.GroupCount item : q.getGroupCountsList()) {
            items.add(GroupCount.fromInternal(item));
        }
        return new GroupCountsResult(items);
    }

    static List<GroupCount> defaultItems() {
        return defaultItems;
    }

    private GroupCountsResult(List<GroupCount> items) {
        this.items = items;
    }

    private static List<GroupCount> defaultItems = new ArrayList<>(0);
    private final List<GroupCount> items;
}
