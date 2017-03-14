package com.pilosa.client.orm;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

public class BatchQuery implements IPqlQuery {
    private Database database = null;
    private List<IPqlQuery> queries = null;

    BatchQuery(Database database) {
        this.database = database;
        this.queries = new ArrayList<>();
    }

    BatchQuery(int queryCount, Database database) {
        this.database = database;
        this.queries = new ArrayList<>(queryCount);
    }

    public Database getDatabase() {
        return this.database;
    }

    public void add(IPqlQuery query) {
        if (query.getDatabase() != this.getDatabase()) {
            throw new PilosaException("Query database should be the same as BatchQuery database");
        }
        this.queries.add(query);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.queries.size());
        for (IPqlQuery query : this.queries) {
            builder.append(query.toString());
        }
        return builder.toString();
    }
}
