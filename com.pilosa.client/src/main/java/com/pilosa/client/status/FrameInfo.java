package com.pilosa.client.status;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pilosa.client.TimeQuantum;

public final class FrameInfo {

    @JsonProperty("Name")
    public String getName() {
        return this.name;
    }

    void setName(String name) {
        this.name = name;
    }

    public String getRowLabel() {
        return this.meta.getRowLabel();
    }

    public TimeQuantum getTimeQuantum() {
        return this.meta.getTimeQuantum();
    }

    public boolean isInverseEnabled() {
        return this.meta.isInverseEnabled();
    }

    @JsonProperty("Meta")
    void setMeta(FrameMeta meta) {
        this.meta = meta;
    }

    private String name;
    private FrameMeta meta;
}

final class FrameMeta {
    String getRowLabel() {
        return this.rowLabel;
    }

    @JsonProperty("RowLabel")
    void setRowLabel(String rowLabel) {
        this.rowLabel = rowLabel;
    }

    TimeQuantum getTimeQuantum() {
        return this.timeQuantum;
    }

    @JsonProperty("TimeQuantum")
    void setTimeQuantum(String s) {
        this.timeQuantum = TimeQuantum.fromString(s);
    }

    boolean isInverseEnabled() {
        return this.inverseEnabled;
    }

    @JsonProperty("InverseEnabled")
    void setInverseEnabled(boolean inverseEnabled) {
        this.inverseEnabled = inverseEnabled;
    }

    private String rowLabel;
    private TimeQuantum timeQuantum = TimeQuantum.NONE;
    private boolean inverseEnabled = false;
}
