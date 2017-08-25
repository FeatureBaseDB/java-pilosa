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

package com.pilosa.client.helpers;

import com.pilosa.client.orm.Index;
import com.pilosa.client.orm.PqlBaseQuery;
import com.pilosa.client.orm.PqlBatchQuery;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class BatchManager implements Iterator<PqlBatchQuery> {
    /**
     * Create a batch manager with the given index and batch size.
     *
     * @param index     The index that bounds the batches in this manager.
     * @param batchSize Maximum number of queries in a batch.
     * @return a Batch Manager.
     */
    public static BatchManager withBatchSize(Index index, final int batchSize) {
        return new BatchManager(index, batchSize);
    }

    /**
     * Adds a query to the current batch.
     *
     * @param query PQL query
     */
    public void add(PqlBaseQuery query) {
        if (this.currentBatch == null || this.currentBatch.size() == this.batchSize) {
            this.currentBatch = this.index.batchQuery(this.batchSize);
            this.batches.addLast(this.currentBatch);
        }
        this.currentBatch.add(query);
    }

    @Override
    public boolean hasNext() {
        return !this.batches.isEmpty();
    }

    @Override
    public PqlBatchQuery next() {
        PqlBatchQuery batch = this.batches.pollFirst();
        if (batch == null) {
            this.currentBatch = null;
        }
        return batch;
    }

    @Override
    public void remove() {
        // JDK 7 compatibility
    }

    private BatchManager(Index index, final int batchSize) {
        this.index = index;
        this.batchSize = batchSize;
    }

    private Deque<PqlBatchQuery> batches = new ConcurrentLinkedDeque<>();
    private final int batchSize;
    private PqlBatchQuery currentBatch = null;
    private Index index;
}
