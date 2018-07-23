/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package com.pilosa.client.orm;

import com.pilosa.client.exceptions.PilosaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains a batch of PQL queries.
 * <p>
 * Use <code>Index.batchQuery</code> method to create an instance.
 * This class is not thread-safe, do not update the same PqlBatchQuery object in different threads.
 * <p>
 *     Usage
 * <pre>
 * <code>
 *     Index repo = Index.create("repository");
 *     Field stargazer = repo.field("stargazer");
 *     PqlBatchQuery query = repo.batchQuery(
 *          stargazer.row(5),
 *          stargazer.row(15),
 *          repo.union(stargazer.row(20), stargazer.row(25)));
 * </code>
 * </pre>
 */
public class PqlBatchQuery implements PqlQuery {
    private Index index = null;
    private List<PqlQuery> queries = null;

    /**
     * Creates a PqlBatchQuery object with the given index.
     *
     * @param index the index this batch query belongs to
     */
    PqlBatchQuery(Index index) {
        this.index = index;
        this.queries = new ArrayList<>();
    }

    /**
     * Creates a PqlBatchQuery object with the given index and query count.
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
    PqlBatchQuery(Index index, int queryCount) {
        this.index = index;
        this.queries = new ArrayList<>(queryCount);
    }

    /**
     * Creates a PqlBatchQuery object with the given index and queries
     * @param index the index this batch query belongs to
     * @param queries queries in the batch
     */
    PqlBatchQuery(Index index, PqlQuery... queries) {
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
            throw new PilosaException("Query index should be the same as PqlBatchQuery index");
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
