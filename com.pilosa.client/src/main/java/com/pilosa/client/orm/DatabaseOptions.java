package com.pilosa.client.orm;

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.Validator;

public final class DatabaseOptions {

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

        public DatabaseOptions build() {
            return new DatabaseOptions(this.columnLabel, this.timeQuantum);
        }

        private String columnLabel = "col_id";
        private TimeQuantum timeQuantum = TimeQuantum.NONE;
    }

    @SuppressWarnings("WeakerAccess")
    public static DatabaseOptions withDefaults() {
        return new Builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getColumnLabel() {
        return this.columnLabel;
    }

    public TimeQuantum getTimeQuantum() {
        return timeQuantum;
    }

    private DatabaseOptions(final String columnLabel, final TimeQuantum timeQuantum) {
        this.columnLabel = columnLabel;
        this.timeQuantum = timeQuantum;
    }

    private final String columnLabel;
    private final TimeQuantum timeQuantum;
}
