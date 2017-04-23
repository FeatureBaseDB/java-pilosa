package com.pilosa.client.orm;

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.Validator;

public final class IndexOptions {

    public static class Builder {
        private Builder() {
        }

        public Builder setColumnLabel(String columnLabel) {
            Validator.ensureValidLabel(columnLabel);
            this.columnLabel = columnLabel;
            return this;
        }

        public Builder setTimeQuantum(TimeQuantum timeQuantum) {
            this.timeQuantum = timeQuantum;
            return this;
        }

        public IndexOptions build() {
            return new IndexOptions(this.columnLabel, this.timeQuantum);
        }

        private String columnLabel = "col_id";
        private TimeQuantum timeQuantum = TimeQuantum.NONE;
    }

    @SuppressWarnings("WeakerAccess")
    public static IndexOptions withDefaults() {
        return new Builder().build();
    }

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
