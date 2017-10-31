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

import com.pilosa.client.orm.PqlQuery;

import java.util.*;

/**
 * Contains options to customize {@link PilosaClient#query(PqlQuery, QueryOptions)}.
 * <p>
 * In order to set options, create a {@link Builder} object using {@link QueryOptions#builder()}.
 * <p>
 * <pre>
 *  <code>
 *     QueryOptions options = QueryOptions.builder()
 *         .setColumns(true)
 *         .build();
 *  </code>
 * </pre>
 *
 * @see <a href="https://www.pilosa.com/docs/api-reference/">Pilosa API Reference</a>
 */
public class QueryOptions {
    public static class Builder {
        private Builder() {
        }

        /**
         * Enables returning column data from bitmap queries.
         *
         * @param columns set to <code>true</code> for returning column data
         * @return QueryOptions builder
         */
        public Builder setColumns(boolean columns) {
            this.columns = columns;
            return this;
        }

        /**
         * Disables returning bits from bitmap queries.
         *
         * @param exclude set to <code>true</code> for excluding (not returning) bits
         * @return QueryOptions builder
         */
        public Builder setExcludeBits(boolean exclude) {
            this.excludeBits = exclude;
            return this;
        }

        /**
         * Disables returning attributes from bitmap queries.
         *
         * @param exclude set to <code>true</code> for excluding (not returning) attributes
         * @return QueryOptions builder
         */
        public Builder setExcludeAttributes(boolean exclude) {
            this.excludeAttributes = exclude;
            return this;
        }

        /**
         * Restricts query to a subset of slices.
         *
         * @param slices set to a list of slices to restrict query to.
         * @return QueryOptions builder
         */
        public Builder setSlices(List<Long> slices) {
            this.slices = slices;
            return this;
        }

        /**
         * Creates the QueryOptions object.
         *
         * @return QueryOptions object
         */
        public QueryOptions build() {
            return new QueryOptions(this.columns, this.excludeBits, this.excludeAttributes, this.slices);
        }

        private boolean columns = false;
        private boolean excludeBits;
        private boolean excludeAttributes;
        private List<Long> slices = new ArrayList<Long>();
    }

    /**
     * Creates a QueryOptions object with the defaults.
     * @return QueryOptions object
     */
    @SuppressWarnings("WeakerAccess")
    public static QueryOptions defaultOptions() {
        return new Builder().build();
    }

    @SuppressWarnings("WeakerAccess")
    public boolean isColumns() {
        return this.columns;
    }

    public boolean isExcludeBits() {
        return this.excludeBits;
    }

    public boolean isExcludeAttributes() {
        return this.excludeAttributes;
    }

    public List<Long> getSlices() {
        return this.slices;
    }

    /**
     * Creates a QueryOptions.Builder object.
     * @return a Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    private QueryOptions(boolean columns, boolean excludeBits, boolean excludeAttributes, List<Long> slices) {
        this.columns = columns;
        this.excludeBits = excludeBits;
        this.excludeAttributes = excludeAttributes;
        this.slices = slices;
    }

    private final boolean columns;
    private final boolean excludeBits;
    private final boolean excludeAttributes;
    private final List<Long> slices;
}
