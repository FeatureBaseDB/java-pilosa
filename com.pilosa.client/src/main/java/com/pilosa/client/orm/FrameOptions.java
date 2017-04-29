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
 * Contains options to customize {@link Frame} objects and frame queries.
 * <p>
 * In order to set options, create a {@link Builder} object using {@link FrameOptions#builder()}:
 * <p>
 * <pre>
 *  <code>
 *     FrameOptions options = FrameOptions.builder()
 *         .setRowLabel("row-id")
 *         .setTimeQuantum(TimeQuantum.YEAR)
 *         .build();
 *  </code>
 * </pre>
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/">Data Model</a>
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public final class FrameOptions {
    public static class Builder {
        private Builder() {
        }

        /**
         * Sets the row label.
         *
         * @param rowLabel a valid row label. See {@link Validator#ensureValidLabel(String)} for constraints on labels.
         * @return FrameOptions builder
         * @throws com.pilosa.client.exceptions.ValidationException if the row label is invalid.
         */
        public Builder setRowLabel(String rowLabel) {
            Validator.ensureValidLabel(rowLabel);
            this.rowLabel = rowLabel;
            return this;
        }

        /**
         * Sets the time quantum for the frame.
         * <p>
         *     If a Frame has a time quantum, then Views are generated
         *     for each of the defined time segments.
         *
         * @param timeQuantum See {@link TimeQuantum} for valid values.
         * @return FrameOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#time-quantum">Time Quantum</a>
         * @see <a href="https://www.pilosa.com/docs/data-model/#view">View</a>
         */
        public Builder setTimeQuantum(TimeQuantum timeQuantum) {
            this.timeQuantum = timeQuantum;
            return this;
        }

        /**
         * Enables inverse frames.
         *
         * @param enabled Set to <code>true</code> to enable.
         * @return FrameOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#view">View</a>
         */
        public Builder setInverseEnabled(boolean enabled) {
            this.inverseEnabled = enabled;
            return this;
        }

        /**
         * Creates the FrameOptions object.
         *
         * @return FrameOptions object
         */
        public FrameOptions build() {
            return new FrameOptions(this.rowLabel, this.timeQuantum, this.inverseEnabled);
        }

        private String rowLabel = "rowID";
        private TimeQuantum timeQuantum = TimeQuantum.NONE;
        private boolean inverseEnabled = false;
    }

    /**
     * Creates a FrameOptions object with defaults.
     *
     * @return FrameOptions object
     */
    @SuppressWarnings("WeakerAccess")
    public static FrameOptions withDefaults() {
        return new Builder().build();
    }

    @SuppressWarnings("WeakerAccess")
    public String getRowLabel() {
        return this.rowLabel;
    }

    /**
     * Creates a FrameBuilder.Builder object.
     *
     * @return a Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    public TimeQuantum getTimeQuantum() {
        return this.timeQuantum;
    }

    @SuppressWarnings("WeakerAccess")
    public boolean isInverseEnabled() {
        return this.inverseEnabled;
    }

    @Override
    public String toString() {
        return String.format("{\"options\":{\"rowLabel\":\"%s\", \"inverseEnabled\": %s}}",
                this.rowLabel,
                this.inverseEnabled ? "true" : "false");
    }

    private FrameOptions(final String rowLabel, final TimeQuantum timeQuantum,
                         final boolean inverseEnabled) {
        this.rowLabel = rowLabel;
        this.timeQuantum = timeQuantum;
        this.inverseEnabled = inverseEnabled;
    }

    private final String rowLabel;
    private final TimeQuantum timeQuantum;
    private final boolean inverseEnabled;
}
