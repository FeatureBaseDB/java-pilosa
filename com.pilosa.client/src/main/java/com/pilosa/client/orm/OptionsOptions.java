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

package com.pilosa.client.orm;

public class OptionsOptions {
    public static class Builder {
        private Builder() {
        }

        public Builder setColumnAttrs(boolean columnAttrs) {
            this.columnAttrs = columnAttrs;
            return this;
        }

        public Builder setExcludeColumns(boolean excludeColumns) {
            this.excludeColumns = excludeColumns;
            return this;
        }

        public Builder setExcludeRowAttrs(boolean excludeRowAttrs) {
            this.excludeRowAttrs = excludeRowAttrs;
            return this;
        }

        public Builder setShards(long... shards) {
            this.shards = shards;
            return this;
        }

        public OptionsOptions build() {
            return new OptionsOptions(
                    columnAttrs,
                    excludeColumns,
                    excludeRowAttrs,
                    shards);
        }

        private boolean columnAttrs = false;
        private boolean excludeColumns = false;
        private boolean excludeRowAttrs = false;
        private long shards[] = null;

    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isColumnAttrs() {
        return this.columnAttrs;
    }

    public boolean isExcludeColumns() {
        return this.excludeColumns;
    }

    public boolean isExcludeRowAttrs() {
        return this.excludeRowAttrs;
    }

    public long[] getShards() {
        return this.shards;
    }

    public String serialize() {
        StringBuilder b = new StringBuilder();
        b.append(String.format("columnAttrs=%b,excludeColumns=%b,excludeRowAttrs=%b",
                this.columnAttrs, this.excludeColumns, this.excludeRowAttrs));
        if (this.shards.length > 0) {
            b.append(String.format(",shards=[%d", this.shards[0]));
            for (int i = 1; i < this.shards.length; i++) {
                b.append(String.format(",%d", this.shards[i]));
            }
            b.append("]");
        }
        return b.toString();
    }

    OptionsOptions(boolean columnAttrs, boolean excludeColumns,
                   boolean excludeRowAttrs, long shards[]) {
        this.columnAttrs = columnAttrs;
        this.excludeColumns = excludeColumns;
        this.excludeRowAttrs = excludeRowAttrs;
        if (shards == null) {
            this.shards = new long[]{};
        } else {
            this.shards = shards;
        }
    }

    private final boolean columnAttrs;
    private final boolean excludeColumns;
    private final boolean excludeRowAttrs;
    private final long shards[];

}
