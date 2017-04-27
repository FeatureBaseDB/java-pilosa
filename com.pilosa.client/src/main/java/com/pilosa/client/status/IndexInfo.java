package com.pilosa.client.status;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pilosa.client.TimeQuantum;

import java.util.ArrayList;
import java.util.List;

public final class IndexInfo {
    IndexInfo() {
    }

    @JsonProperty("Name")
    public String getName() {
        return this.name;
    }

    void setName(String name) {
        this.name = name;
    }

    public String getColumnLabel() {
        return this.meta.getColumnLabel();
    }

    public TimeQuantum getTimeQuantum() {
        return this.meta.getTimeQuantum();
    }

    @JsonProperty("Frames")
    public List<FrameInfo> getFrames() {
        return this.frames;
    }

    public void setFrames(List<FrameInfo> frames) {
        this.frames = frames;
    }

    @JsonProperty("Meta")
    void setMeta(IndexMeta meta) {
        this.meta = meta;
    }

    private String name;
    private List<FrameInfo> frames = new ArrayList<>();
    private IndexMeta meta;
}

final class IndexMeta {
    @JsonProperty("ColumnLabel")
    String getColumnLabel() {
        return this.columnLabel;
    }

    void setColumnLabel(String columnLabel) {
        this.columnLabel = columnLabel;
    }

    @JsonProperty("TimeQuantum")
    TimeQuantum getTimeQuantum() {
        return this.timeQuantum;
    }

    void setTimeQuantum(String s) {
        this.timeQuantum = TimeQuantum.fromString(s);
    }

    private String columnLabel;
    private TimeQuantum timeQuantum = TimeQuantum.NONE;
}

