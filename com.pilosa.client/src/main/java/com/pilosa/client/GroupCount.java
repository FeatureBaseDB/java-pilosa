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

public final class GroupCount {
    public static GroupCount create(List<FieldRow> groups, long count) {
        return new GroupCount(groups, count);
    }

    public final List<FieldRow> getGroups() {
        return this.groups;
    }

    public long getCount() {
        return this.count;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof GroupCount)) {
            return false;
        }
        GroupCount rhs = (GroupCount) obj;
        return new EqualsBuilder()
                .append(this.groups, rhs.groups)
                .append(this.count, rhs.count)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.groups)
                .append(this.count)
                .toHashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (FieldRow fieldRow : this.groups) {
            builder.append(fieldRow.toString());
        }
        return String.format("GroupCount(groups=[%s], count=%d)",
                builder.toString(), this.count);
    }

    static GroupCount fromInternal(Internal.GroupCount q) {
        List<FieldRow> fieldRows = new ArrayList<>(q.getGroupCount());
        for (Internal.FieldRow fieldRow : q.getGroupList()) {
            fieldRows.add(FieldRow.fromInternal(fieldRow));
        }
        return new GroupCount(fieldRows, q.getCount());
    }

    private GroupCount(List<FieldRow> groups, long count) {
        this.groups = groups;
        this.count = count;
    }

    private final List<FieldRow> groups;
    private final long count;
}
