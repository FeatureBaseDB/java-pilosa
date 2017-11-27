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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.ValidationException;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * The purpose of the Index is to represent a data namespace.
 * <p>
 * You cannot perform cross-index queries. Column-level attributes are global to the Index.
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/">Data Model</a>
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public class Index {
    /**
     * Create an index with a name using defaults.
     *
     * @param name index name
     * @return a Index object
     * @throws ValidationException if the passed index name is not valid
     */
    public static Index withName(String name) {
        return Index.withName(name, IndexOptions.withDefaults());
    }

    /**
     * Creates an index with a name and options.
     *
     * @param name    index name
     * @param options index options
     * @return a Index object
     * @deprecated Index options are deprecated and will be removed in a future release.
     * @throws ValidationException if the passed index name is not valid
     */
    public static Index withName(String name, IndexOptions options) {
        Validator.ensureValidIndexName(name);
        Validator.ensureValidLabel(options.getColumnLabel());
        return new Index(name, options);
    }

    public String getName() {
        return this.name;
    }

    public IndexOptions getOptions() {
        return this.options;
    }

    /**
     * Returns a frame object with the specified name and defaults.
     *
     * @param name frame name
     * @return a Frame object
     * @throws ValidationException if the passed frame name is not valid
     */
    public Frame frame(String name) {
        return this.frame(name, FrameOptions.withDefaults());
    }

    /**
     * Returns a frame with the specified name and options.
     *
     * @param name    frame name
     * @param options frame options
     * @return a Frame object
     * @throws ValidationException if the passed frame name is not valid
     */
    public Frame frame(String name, FrameOptions options) {
        if (this.frames.containsKey(name)) {
            return this.frames.get(name);
        }
        Frame frame = Frame.create(this, name, options);
        this.frames.put(name, frame);
        return frame;
    }

    /**
     * Copies other frame to this index and returns the new frame
     *
     * @param other frame
     * @return copied frame
     */
    public Frame frame(Frame other) {
        return frame(other.getName(), other.getOptions());
    }

    /**
     * Creates a batch query.
     *
     * @return batch query
     */
    public PqlBatchQuery batchQuery() {
        return new PqlBatchQuery(this);
    }

    /**
     * Creates a batch query which has the specified query count pre-allocated.
     * <p>
     *     If the number of queries in the batch is known beforehand, calling this constructor
     *     is more efficient than calling it without the query count.
     *
     *     Note that <code>queryCount</code> is not the limit of queries in the batch; the batch can
     *     grow beyond that.
     *
     * @param queryCount number of queries expected to be in the batch
     * @return batch query
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBatchQuery batchQuery(int queryCount) {
        return new PqlBatchQuery(this, queryCount);
    }

    /**
     * Creates a batch query with the given queries
     *
     * @param queries the queries in the batch
     * @return PqlBatchQuery
     */
    public PqlBatchQuery batchQuery(PqlQuery... queries) {
        return new PqlBatchQuery(this, queries);
    }

    /**
     * Creates a raw query.
     * <p>
     * Note that the query is not validated before sending to the server.
     *
     * @param query raw query
     * @return a Pql query
     */
    public PqlBaseQuery rawQuery(String query) {
        return new PqlBaseQuery(query, this);
    }

    /**
     * Creates a Union query.
     * <p>
     * Union performs a logical OR on the results of each BITMAP_CALL query passed to it.
     *
     * @param bitmaps 2 or more bitmaps to union
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#union">Union Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery union(PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Union", bitmaps);
    }

    /**
     * Creates an Intersect query.
     * <p>
     * Intersect performs a logical AND on the results of each BITMAP_CALL query passed to it.
     *
     * @param bitmaps 2 or more bitmaps to intersect
     * @return a PQL query
     * @throws IllegalArgumentException if the number of bitmaps is less than 1
     * @see <a href="https://www.pilosa.com/docs/query-language/#intersect">Intersect Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery intersect(PqlBitmapQuery... bitmaps) {
        if (bitmaps.length < 1) {
            throw new IllegalArgumentException("Intersect operation requires at least 1 bitmap");
        }
        return bitmapOperation("Intersect", bitmaps);
    }

    /**
     * Creates a Difference query.
     * <p>
     * Difference returns all of the bits from the first BITMAP_CALL argument
     *   passed to it, without the bits from each subsequent BITMAP_CALL.
     *
     * @param bitmaps 1 or more bitmaps to differentiate
     * @return a PQL query
     * @throws IllegalArgumentException if the number of bitmaps is less than 1
     * @see <a href="https://www.pilosa.com/docs/query-language/#difference">Difference Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery difference(PqlBitmapQuery... bitmaps) {
        if (bitmaps.length < 1) {
            throw new IllegalArgumentException("Difference operation requires at least 1 bitmap");
        }
        return bitmapOperation("Difference", bitmaps);
    }

    /**
     * Creates an Xor query.
     *
     * @param bitmaps 2 or more bitmaps to xor
     * @return a PQL query
     * @throws IllegalArgumentException if the number of bitmaps is less than 2
     * @see <a href="https://www.pilosa.com/docs/query-language/#xor">Xor Query</a>
     */
    public PqlBitmapQuery xor(PqlBitmapQuery... bitmaps) {
        if (bitmaps.length < 2) {
            throw new IllegalArgumentException("Difference operation requires at least 2 bitmaps");
        }
        return bitmapOperation("Xor", bitmaps);
    }

    /**
     * Creates a Count query.
     * <p>
     * Count returns the number of set bits in the BITMAP_CALL passed in.
     *
     * @param bitmap the bitmap query
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#count">Count Query</a>
     */
    public PqlBaseQuery count(PqlBitmapQuery bitmap) {
        return pqlQuery(String.format("Count(%s)", bitmap.serialize()));
    }

    /**
     * Creates a SetColumnAttrs query.
     * <p>
     * SetColumnAttrs associates arbitrary key/value pairs with a column in an index.
     * <p>
     * Following object types are accepted:
     *     <ul>
     *         <li>Long</li>
     *         <li>String</li>
     *         <li>Boolean</li>
     *         <li>Double</li>
     *     </ul>
     *
     * @param id         column ID
     * @param attributes column attributes
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setcolumnattrs">SetColumnAttrs Query</a>
     */
    public PqlBaseQuery setColumnAttrs(long id, Map<String, Object> attributes) {
        String attributesString = Util.createAttributesString(this.mapper, attributes);
        return pqlQuery(String.format("SetColumnAttrs(%s=%d, %s)",
                this.options.getColumnLabel(), id, attributesString));
    }

    public Map<String, Frame> getFrames() {
        return this.frames;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Index)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Index rhs = (Index) obj;
        return rhs.name.equals(this.name) &&
                rhs.options.equals(this.options) &&
                rhs.frames.equals(this.frames);
    }

    @Override
    public int hashCode() {
        // note that we don't include frames in the hash
        return new HashCodeBuilder(31, 47)
                .append(this.name)
                .append(this.options)
                .toHashCode();
    }

    PqlBaseQuery pqlQuery(String query) {
        return new PqlBaseQuery(query, this);
    }

    PqlBitmapQuery pqlBitmapQuery(String query) {
        return new PqlBitmapQuery(query, this);
    }

    Index(Index index) {
        // we don't copy index options, since IndexOptions has no mutating methods
        this(index.name, index.options);
        for (Map.Entry<String, Frame> entry : index.frames.entrySet()) {
            // we don't copy frame options, since FrameOptions has no mutating methods
            this.frame(entry.getKey(), entry.getValue().getOptions());
        }
    }

    private PqlBitmapQuery bitmapOperation(String name, PqlBitmapQuery... bitmaps) {
        if (bitmaps.length == 0) {
            return pqlBitmapQuery(String.format("%s()", name));
        }
        StringBuilder builder = new StringBuilder(bitmaps.length - 1);
        builder.append(bitmaps[0].serialize());
        for (int i = 1; i < bitmaps.length; i++) {
            builder.append(", ");
            builder.append(bitmaps[i].serialize());
        }
        return pqlBitmapQuery(String.format("%s(%s)", name, builder.toString()));
    }

    private Index(String name, IndexOptions options) {
        this.name = name;
        this.options = options;
    }

    private String name;
    private IndexOptions options;
    private ObjectMapper mapper = new ObjectMapper();
    private Map<String, Frame> frames = new HashMap<>();
}
