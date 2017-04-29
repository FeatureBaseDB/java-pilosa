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

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.Validator;

/**
 * Contains options to customize {@link Index} objects and column queries.
 * <p>
 * In order to set options, create a {@link Builder} object using {@link IndexOptions#builder()}:
 * <p>
 * <pre>
 *  <code>
 *     IndexOptions options = IndexOptions.builder()
 *         .setColumnLabel("col-id")
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
         * Sets the column label.
         *
         * @param columnLabel a valid column label. See {@link Validator#ensureValidLabel(String)} for constraints on labels.
         * @return IndexOptions builder
         * @throws com.pilosa.client.exceptions.ValidationException if the column label is invalid.
         */
        public Builder setColumnLabel(String columnLabel) {
            Validator.ensureValidLabel(columnLabel);
            this.columnLabel = columnLabel;
            return this;
        }

        /**
         * Sets the time quantum.
         *
         * @param timeQuantum See {@link TimeQuantum} for valid values.
         * @return IndexOptions builder.
         * @see <a href="https://www.pilosa.com/docs/data-model/#time-quantum">Time Quantum</a>
         */
        public Builder setTimeQuantum(TimeQuantum timeQuantum) {
            this.timeQuantum = timeQuantum;
            return this;
        }

        /**
         * Creates the IndexOptions object.
         *
         * @return IndexOptions object
         */
        public IndexOptions build() {
            return new IndexOptions(this.columnLabel, this.timeQuantum);
        }

        private String columnLabel = "columnID";
        private TimeQuantum timeQuantum = TimeQuantum.NONE;
    }

    /**
     * Creates an IndexOptions object with defaults.
     *
     * @return IndexOptions object
     */
    @SuppressWarnings("WeakerAccess")
    public static IndexOptions withDefaults() {
        return new Builder().build();
    }

    /**
     * Creates an IndexBuilder.Builder object.
     *
     * @return a Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("WeakerAccess")
    public String getColumnLabel() {
        return this.columnLabel;
    }

    public TimeQuantum getTimeQuantum() {
        return timeQuantum;
    }

    @Override
    public String toString() {
        return String.format("{\"options\":{\"columnLabel\":\"%s\"}}",
                this.columnLabel);
    }

    private IndexOptions(final String columnLabel, final TimeQuantum timeQuantum) {
        this.columnLabel = columnLabel;
        this.timeQuantum = timeQuantum;
    }

    private final String columnLabel;
    private final TimeQuantum timeQuantum;
}
