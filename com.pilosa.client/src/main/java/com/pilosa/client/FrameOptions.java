package com.pilosa.client;

public class FrameOptions {
    private String rowLabel = "id";

    private FrameOptions() {}

    public static FrameOptions withDefaults() {
        return new FrameOptions();
    }

    public static FrameOptions withRowLabel(String rowLabel) {
        Validator.ensureValidLabel(rowLabel);
        FrameOptions options = new FrameOptions();
        options.rowLabel = rowLabel;
        return options;
    }

    public String getRowLabel() {
        return this.rowLabel;
    }

}
