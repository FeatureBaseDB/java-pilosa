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

/**
 * Represents a result from {@link com.pilosa.client.orm.Frame#topN(long)} call.
 *
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public final class CountResultItem {
    /**
     * Returns the row ID.
     *
     * @return row ID
     */
    public long getID() {
        return this.id;
    }

    /**
     * Returns the row key (Enterprise version)
     *
     * @return row key
     */
    public String getKey() {
        return this.key;
    }

    /**
     * Returns the count of column IDs where this bitmap item is 1.
     *
     * @return count of column IDs where this bitmap item is 1
     */
    public long getCount() {
        return this.count;
    }

    @Override
    public String toString() {
        if (this.key.isEmpty()) {
            return String.format("CountResultItem(id=%d, count=%d)",
                    this.id, this.count);
        }
        return String.format("CountResultItem(key=\"%s\", count=%d)",
                this.key, this.count);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CountResultItem)) {
            return false;
        }
        CountResultItem rhs = (CountResultItem) obj;
        return new EqualsBuilder()
                .append(this.id, rhs.id)
                .append(this.key, rhs.key)
                .append(this.count, rhs.count)
                .isEquals();

    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.id)
                .append(key)
                .append(this.count)
                .toHashCode();
    }

    static CountResultItem create(long id, String key, long count) {
        CountResultItem item = new CountResultItem();
        item.id = (key.equals("") ? id : 0);
        item.key = key;
        item.count = count;
        return item;
    }

    static CountResultItem fromInternal(Internal.Pair pair) {
        return CountResultItem.create(pair.getID(), pair.getKey(), pair.getCount());
    }

    private long id;
    private String key;
    private long count;
}
