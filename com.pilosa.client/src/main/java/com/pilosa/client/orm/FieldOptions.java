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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.TimeQuantum;
import com.pilosa.client.exceptions.PilosaException;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains options to customize {@link Field} objects and frame queries.
 * <p>
 * In order to set options, create a {@link Builder} object using {@link FieldOptions#builder()}:
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
public final class FieldOptions {
    public static class Builder {
        private Builder() {
        }

        /**
         * @return FieldOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Field</a>
         */
        public Builder fieldSet() {
            return fieldSet(CacheType.DEFAULT, 0);
        }

        /**
         * @param cacheType CacheType.DEFAULT, CacheType.LRU or CacheType.RANKED
         * @return FieldOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Field</a>
         */
        public Builder fieldSet(CacheType cacheType) {
            return fieldSet(cacheType, 0);
        }

        /**
         * @param cacheType CacheType.DEFAULT, CacheType.LRU or CacheType.RANKED
         * @param cacheSize Values greater than 0 sets the cache size. Otherwise uses the default cache size.
         * @return FieldOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Field</a>
         */
        public Builder fieldSet(CacheType cacheType, int cacheSize) {
            this.fieldType = FieldType.SET;
            this.cacheType = cacheType;
            this.cacheSize = cacheSize;
            return this;
        }

        /**
         * Adds an integer field to the frame options
         *
         * @param min  Minimum value this field can represent.
         * @param max  Maximum value this field can represent.
         * @return FieldOptions builder
         * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Field</a>
         */
        public Builder fieldInt(long min, long max) {
            this.fieldType = FieldType.INT;
            this.min = min;
            this.max = max;
            return this;
        }

        public Builder fieldTime(TimeQuantum timeQuantum) {
            this.fieldType = FieldType.TIME;
            this.timeQuantum = timeQuantum;
            return this;
        }

        /**
         * Creates the FieldOptions object.
         *
         * @return FieldOptions object
         */
        public FieldOptions build() {
            return new FieldOptions(this.timeQuantum,
                    this.cacheType,
                    this.cacheSize,
                    this.fieldType,
                    this.min,
                    this.max);
        }

        private TimeQuantum timeQuantum = TimeQuantum.NONE;
        private CacheType cacheType = CacheType.DEFAULT;
        private int cacheSize = 0;
        private FieldType fieldType = FieldType.DEFAULT;
        private long min = 0;
        private long max = 0;

    }

    /**
     * Creates a FieldOptions object with defaults.
     *
     * @return FieldOptions object
     */
    @SuppressWarnings("WeakerAccess")
    public static FieldOptions withDefaults() {
        return new Builder().build();
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

    public CacheType getCacheType() {
        return this.cacheType;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public long getMin() {
        return this.min;
    }

    public long getMax() {
        return this.max;
    }

    @Override
    public String toString() {
        Map<String, Object> options = new HashMap<>();
        if (this.fieldType != FieldType.DEFAULT) {
            options.put("type", this.fieldType.toString());
        }
        switch (this.fieldType) {
            case SET:
                if (!this.cacheType.equals(CacheType.DEFAULT)) {
                    options.put("cacheType", this.cacheType.toString());
                }
                if (this.cacheSize > 0) {
                    options.put("cacheSize", this.cacheSize);
                }
                break;
            case INT:
                options.put("min", this.min);
                options.put("max", this.max);
                break;
            case TIME:
                options.put("timeQuantum", this.timeQuantum.toString());
        }
        Map<String, Object> optionsRoot = new HashMap<>(1);
        optionsRoot.put("options", options);
        try {
            return mapper.writeValueAsString(optionsRoot);
        } catch (JsonProcessingException e) {
            throw new PilosaException("Options cannot be serialized");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FieldOptions)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        FieldOptions rhs = (FieldOptions) obj;
        return rhs.timeQuantum.equals(this.timeQuantum) &&
                rhs.cacheType.equals(this.cacheType) &&
                rhs.cacheSize == this.cacheSize &&
                rhs.fieldType == this.fieldType &&
                rhs.min == this.min &&
                rhs.max == this.max;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.timeQuantum)
                .append(this.cacheType)
                .append(this.cacheSize)
                .append(this.fieldType)
                .append(this.min)
                .append(this.max)
                .toHashCode();
    }

    private FieldOptions(final TimeQuantum timeQuantum,
                         final CacheType cacheType,
                         final int cacheSize,
                         final FieldType fieldType,
                         final long min,
                         final long max) {
        this.timeQuantum = timeQuantum;
        this.cacheType = cacheType;
        this.cacheSize = cacheSize;
        this.fieldType = fieldType;
        this.min = min;
        this.max = max;
    }

    static {
        ObjectMapper m = new ObjectMapper();
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper = m;
    }

    private static final ObjectMapper mapper;
    private final TimeQuantum timeQuantum;
    private final CacheType cacheType;
    private final int cacheSize;
    private final FieldType fieldType;
    private final long min;
    private final long max;
}
