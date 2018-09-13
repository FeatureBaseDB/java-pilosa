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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Contains options to customize {@link Field} objects and field queries.
 * <p>
 * In order to set options, create a {@link Builder} object using {@link IndexOptions#builder()}:
 * <p>
 * <pre>
 *  <code>
 *     FieldOptions options = FieldOptions.builder()
 *         .setInverseEnabled(true)
 *         .setTimeQuantum(TimeQuantum.YEAR)
 *         .build();
 *  </code>
 * </pre>
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/">Data Model</a>
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public final class IndexOptions {
    public static class Builder {
        private Builder() {
        }

        /**
         * Sets whether the index uses string keys.
         *
         * @param enable Enables string keys for this field if set to true.
         * @return IndexOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#index">Pilosa Data Model: Index</a>
         */
        public Builder keys(boolean enable) {
            this.keys = enable;
            return this;
        }

        /**
         * Enables keeping track of existence which is required for Not query
         *
         * @param enable
         * @return IndexOptions builder
         */

        public Builder trackExistence(boolean enable) {
            this.trackExistence = enable;
            return this;
        }

        /**
         * Creates the FieldOptions object.
         *
         * @return FieldOptions object
         */
        public IndexOptions build() {
            return new IndexOptions(this.keys, this.trackExistence);
        }

        private boolean keys = false;
        private boolean trackExistence = false;

    }

    /**
     * Creates a FieldOptions object with defaults.
     *
     * @return FieldOptions object
     */
    @SuppressWarnings("WeakerAccess")
    public static IndexOptions withDefaults() {
        return new Builder().build();
    }

    /**
     * Creates a FieldBuilder.Builder object.
     *
     * @return a Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    public boolean isKeys() {
        return this.keys;
    }

    public boolean isTrackExistence() {
        return this.trackExistence;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"options\":{");
        builder.append("\"keys\":");
        builder.append(this.keys ? "true" : "false");
        builder.append(",\"trackExistence\":");
        builder.append(this.trackExistence ? "true" : "false");
        builder.append("}}");
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IndexOptions)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        IndexOptions rhs = (IndexOptions) obj;
        return rhs.keys == this.keys;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.keys)
                .toHashCode();
    }

    private IndexOptions(final boolean keys, boolean trackExistence) {
        this.keys = keys;
        this.trackExistence = trackExistence;
    }

    static {
        ObjectMapper m = new ObjectMapper();
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper = m;
    }

    private static final ObjectMapper mapper;
    private final boolean keys;
    private final boolean trackExistence;
}
