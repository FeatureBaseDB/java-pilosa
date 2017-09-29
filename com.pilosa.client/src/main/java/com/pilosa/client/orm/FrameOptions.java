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
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Contains options to customize {@link Frame} objects and frame queries.
 * <p>
 * In order to set options, create a {@link Builder} object using {@link FrameOptions#builder()}:
 * <p>
 * <pre>
 *  <code>
 *     FrameOptions options = FrameOptions.builder()
 *         .setInverseEnabled(true)
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
         * @deprecated Row labels are deprecated and will be removed in a future release.
         */
        @Deprecated
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
         * @see <a href="https://www.pilosa.com/docs/data-model/#view">Pilosa Data Model: View</a>
         */
        public Builder setTimeQuantum(TimeQuantum timeQuantum) {
            this.timeQuantum = timeQuantum;
            return this;
        }

        /**
         * Enables inverted frames.
         *
         * @param enabled Set to <code>true</code> to enable.
         * @return FrameOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#view">Pilosa Data Model: View</a>
         */
        public Builder setInverseEnabled(boolean enabled) {
            this.inverseEnabled = enabled;
            return this;
        }


        /**
         * Sets the cache type for the frame.
         *
         * @param cacheType CacheType.DEFAULT, CacheType.LRU or CacheType.RANKED
         * @return FrameOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Frame</a>
         */
        public Builder setCacheType(CacheType cacheType) {
            this.cacheType = cacheType;
            return this;
        }

        /**
         * Sets the cache size for the frame
         *
         * @param cacheSize Values greater than 0 sets the cache size. Otherwise uses the default cache size.
         * @return FrameOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Frame</a>
         */
        public Builder setCacheSize(int cacheSize) {
            this.cacheSize = cacheSize;
            return this;
        }

        /**
         * Adds an integer field to the frame options
         *
         * @param name Name of the field.
         * @param min  Minimum value this field can represent.
         * @param max  Maximum value this field can represent.
         * @return FrameOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Frame</a>
         */
        public Builder addIntField(String name, long min, long max) {
            this.fields.put(name, RangeFieldInfo.intField(name, min, max));
            return this;
        }

        /**
         * Creates the FrameOptions object.
         *
         * @return FrameOptions object
         */
        public FrameOptions build() {
            return new FrameOptions(this.rowLabel, this.timeQuantum,
                    this.inverseEnabled, this.cacheType, this.cacheSize,
                    this.fields);
        }

        private String rowLabel = "rowID";
        private TimeQuantum timeQuantum = TimeQuantum.NONE;
        private boolean inverseEnabled = false;
        private CacheType cacheType = CacheType.DEFAULT;
        private int cacheSize = 0;
        private Map<String, RangeFieldInfo> fields = new HashMap<>();

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
    @Deprecated
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

    public CacheType getCacheType() {
        return this.cacheType;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public boolean isRangeEnabled() {
        return this.fields.size() > 0;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"options\": {");
        builder.append(String.format("\"rowLabel\":\"%s\"", this.rowLabel));
        if (this.inverseEnabled) {
            builder.append(",\"inverseEnabled\":true");
        }
        if (!this.timeQuantum.equals(TimeQuantum.NONE)) {
            builder.append(String.format(",\"timeQuantum\":\"%s\"", this.timeQuantum.toString()));
        }
        if (!this.cacheType.equals(CacheType.DEFAULT)) {
            builder.append(String.format(",\"cacheType\":\"%s\"", this.cacheType.toString()));
        }
        if (this.cacheSize > 0) {
            builder.append(String.format(",\"cacheSize\":%d", this.cacheSize));
        }
        if (this.fields.size() > 0) {
            builder.append(",\"rangeEnabled\":true");
            builder.append(",\"fields\":[");
            Iterator<Map.Entry<String, RangeFieldInfo>> iter = this.fields.entrySet().iterator();
            Map.Entry<String, RangeFieldInfo> entry = iter.next();
            builder.append(entry.getValue());
            while (iter.hasNext()) {
                entry = iter.next();
                builder.append(",");
                builder.append(entry.getValue());
            }
            builder.append("]");
        }
        builder.append("}}");
        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FrameOptions)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        FrameOptions rhs = (FrameOptions) obj;
        return rhs.rowLabel.equals(this.rowLabel) &&
                rhs.timeQuantum.equals(this.timeQuantum) &&
                rhs.inverseEnabled == this.inverseEnabled &&
                rhs.cacheType.equals(this.cacheType) &&
                rhs.cacheSize == this.cacheSize &&
                rhs.fields.equals(this.fields);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.rowLabel)
                .append(this.timeQuantum)
                .append(this.inverseEnabled)
                .append(this.cacheType)
                .append(this.cacheSize)
                .append(this.fields)
                .toHashCode();
    }

    private FrameOptions(final String rowLabel, final TimeQuantum timeQuantum,
                         final boolean inverseEnabled,
                         final CacheType cacheType, final int cacheSize,
                         final Map<String, RangeFieldInfo> fields) {
        this.rowLabel = rowLabel;
        this.timeQuantum = timeQuantum;
        this.inverseEnabled = inverseEnabled;
        this.cacheType = cacheType;
        this.cacheSize = cacheSize;
        this.fields = (fields != null) ? fields : new HashMap<String, RangeFieldInfo>();
    }

    private final String rowLabel;
    private final TimeQuantum timeQuantum;
    private final boolean inverseEnabled;
    private final CacheType cacheType;
    private final int cacheSize;
    private final Map<String, RangeFieldInfo> fields;
}
