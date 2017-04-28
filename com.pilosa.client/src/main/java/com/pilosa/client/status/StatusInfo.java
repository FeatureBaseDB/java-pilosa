package com.pilosa.client.status;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public final class StatusInfo {
    @JsonProperty("Nodes")
    public List<NodeInfo> getNodes() {
        return this.nodes;
    }

    void setNodes(List<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    private List<NodeInfo> nodes;
}

