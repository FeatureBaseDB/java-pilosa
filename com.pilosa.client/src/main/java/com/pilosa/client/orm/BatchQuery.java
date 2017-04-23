package com.pilosa.client.orm;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

public class BatchQuery implements PqlQuery {
    private Index index = null;
    private List<PqlQuery> queries = null;

    BatchQuery(Index index) {
        this.index = index;
        this.queries = new ArrayList<>();
    }

    BatchQuery(Index index, int queryCount) {
        this.index = index;
        this.queries = new ArrayList<>(queryCount);
    }

    BatchQuery(Index index, PqlQuery... queries) {
        this(index, queries.length);
        for (PqlQuery query : queries) {
            this.add(query);
        }
    }

    public Index getIndex() {
        return this.index;
    }

    public void add(PqlQuery query) {
        if (query.getIndex() != this.getIndex()) {
            throw new PilosaException("Query index should be the same as BatchQuery index");
        }
        this.queries.add(query);
    }

    public String serialize() {
        StringBuilder builder = new StringBuilder(this.queries.size());
        for (PqlQuery query : this.queries) {
            builder.append(query.serialize());
        }
        return builder.toString();
    }
}
