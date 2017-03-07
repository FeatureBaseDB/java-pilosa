package com.pilosa.client;

public class FrameOptions {
    private String rowLabel = "id";

    private FrameOptions() {}

    public static FrameOptions withDefaults() {
        return new FrameOptions();
    }

    public static FrameOptions withColumnLabel(String columnLabel) {
        Validator.ensureValidLabel(columnLabel);
        FrameOptions options = new FrameOptions();
        options.rowLabel = columnLabel;
        return options;
    }

    public String getRowLabel() {
        return this.rowLabel;
    }

}
