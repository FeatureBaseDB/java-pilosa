package com.pilosa.client.orm;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

public class BatchQuery implements PqlQuery {
    private Database database = null;
    private List<PqlQuery> queries = null;

    BatchQuery(Database database) {
        this.database = database;
        this.queries = new ArrayList<>();
    }

    BatchQuery(Database database, int queryCount) {
        this.database = database;
        this.queries = new ArrayList<>(queryCount);
    }

    BatchQuery(Database database, PqlQuery... queries) {
        this(database, queries.length);
        for (PqlQuery query : queries) {
            this.add(query);
        }
    }

    public Database getDatabase() {
        return this.database;
    }

    public void add(PqlQuery query) {
        if (query.getDatabase() != this.getDatabase()) {
            throw new PilosaException("Query database should be the same as BatchQuery database");
        }
        this.queries.add(query);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(this.queries.size());
        for (PqlQuery query : this.queries) {
            builder.append(query.toString());
        }
        return builder.toString();
    }
}
