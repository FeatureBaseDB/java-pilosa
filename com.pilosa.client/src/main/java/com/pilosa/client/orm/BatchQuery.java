package com.pilosa.client.orm;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains a batch of PQL queries.
 * <p>
 * <p>
 * Use <code>Index.batchQuery</code> method to create an instance.
 * This class is not thread-safe, do not update the same BatchQuery object in different threads.
 */
public class BatchQuery implements PqlQuery {
    private Index index = null;
    private List<PqlQuery> queries = null;

    /**
     * Creates a BatchQuery object with the given index.
     *
     * @param index the index this batch query belongs to
     */
    BatchQuery(Index index) {
        this.index = index;
        this.queries = new ArrayList<>();
    }

    /**
     * Creates a BatchQuery object with the given index and query count.
     *
     * <p>
     *     If the number of queries in the batch is known beforehand, calling this constructor
     *     is more efficient than calling it without the query count.
     *
     *     Note that <code>queryCount</code> is not the limit of queries in the batch; the batch can
     *     grow beyond that.
     * @param index the index this batch query belongs to
     * @param queryCount number of queries expected in the batch
     */
    BatchQuery(Index index, int queryCount) {
        this.index = index;
        this.queries = new ArrayList<>(queryCount);
    }

    /**
     * Creates   a BatchQuery object with the given index and queries
     * @param index the index this batch query belongs to
     * @param queries queries in the batch
     */
    BatchQuery(Index index, PqlQuery... queries) {
        this(index, queries.length);
        for (PqlQuery query : queries) {
            this.add(query);
        }
    }

    public Index getIndex() {
        return this.index;
    }

    /**
     * Adds a query to the batch.
     *
     * <p>
     *  The index of the added query must be same as the batch query.
     *
     * @param query the query to be added
     */
    public void add(PqlQuery query) {
        if (query.getIndex() != this.getIndex()) {
            throw new PilosaException("Query index should be the same as BatchQuery index");
        }
        this.queries.add(query);
    }

    @Override
    public String serialize() {
        StringBuilder builder = new StringBuilder(this.queries.size());
        for (PqlQuery query : this.queries) {
            builder.append(query.serialize());
        }
        return builder.toString();
    }
}
