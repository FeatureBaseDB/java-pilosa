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

        private String columnLabel = "col_id";
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
