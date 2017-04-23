package com.pilosa.client.orm;

import com.pilosa.client.TimeQuantum;
import com.pilosa.client.Validator;

public final class FrameOptions {
    public static class Builder {
        private Builder() {
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

        public Builder setInverseEnabled(boolean enabled) {
            this.inverseEnabled = enabled;
            return this;
        }

        public FrameOptions build() {
            return new FrameOptions(this.rowLabel, this.timeQuantum, this.inverseEnabled);
        }

        private String rowLabel = "id";
        private TimeQuantum timeQuantum = TimeQuantum.NONE;
        private boolean inverseEnabled = false;
    }

    @SuppressWarnings("WeakerAccess")
    public static FrameOptions withDefaults() {
        return new Builder().build();
    }

    public String getRowLabel() {
        return this.rowLabel;
    }

    public static Builder builder() {
        return new Builder();
    }

    public TimeQuantum getTimeQuantum() {
        return this.timeQuantum;
    }

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
