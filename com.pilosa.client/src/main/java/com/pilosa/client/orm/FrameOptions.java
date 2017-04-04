package com.pilosa.client.orm;

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.Validator;

public final class FrameOptions {
    public static class Builder {
        public Builder() {
        }

        public Builder setRowLabel(String rowLabel) {
            Validator.ensureValidLabel(rowLabel);
            this.rowLabel = rowLabel;
            return this;
        }

        public Builder setTimeQuantum(TimeQuantum timeQuantum) {
            this.timeQuantum = timeQuantum;
            return this;
        }

        public FrameOptions build() {
            return new FrameOptions(this.rowLabel, this.timeQuantum);
        }

        private String rowLabel = "id";
        private TimeQuantum timeQuantum = TimeQuantum.NONE;
    }

    public static FrameOptions withDefaults() {
        return new Builder().build();
    }

    public String getRowLabel() {
        return this.rowLabel;
    }

    public TimeQuantum getTimeQuantum() {
        return this.timeQuantum;
    }

    private FrameOptions(final String rowLabel, final TimeQuantum timeQuantum) {
        this.rowLabel = rowLabel;
        this.timeQuantum = timeQuantum;
    }

    private final String rowLabel;
    private final TimeQuantum timeQuantum;
}
