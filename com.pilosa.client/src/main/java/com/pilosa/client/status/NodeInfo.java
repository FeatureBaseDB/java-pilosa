package com.pilosa.client.status;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public final class NodeInfo {
    @JsonProperty("Host")
    public String getHost() {
        return this.host;
    }

    void setHost(String host) {
        this.host = host;
    }

    @JsonProperty("Indexes")
    public List<IndexInfo> getIndexes() {
        return this.indexes;
    }

    void setIndexes(List<IndexInfo> indexes) {
        this.indexes = indexes;
    }

    private String host;
    private List<IndexInfo> indexes;
}
