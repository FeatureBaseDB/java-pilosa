package com.pilosa.client.orm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.ValidationException;

import java.util.Map;

/**
 * The purpose of the Index is to represent a data namespace.
 * <p>
 * You cannot perform
 * cross-index queries. Column-level attributes are global to the Index.
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
     * Creates a index with a name and options.
     *
     * @param name    index name
     * @param options index options
     * @return a Index object
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
     * Creates a frame with the specified name and defaults.
     *
     * @param name frame name
     * @return a Frame object
     * @throws ValidationException if the passed frame name is not valid
     */
    public Frame frame(String name) {
        return Frame.create(this, name, FrameOptions.withDefaults());
    }

    /**
     * Creates a frame with the specified name and options.
     *
     * @param name    frame name
     * @param options frame options
     * @return a Frame object
     * @throws ValidationException if the passed frame name is not valid
     */
    public Frame frame(String name, FrameOptions options) {
        return Frame.create(this, name, options);
    }

    /**
     * Creates a batch query.
     *
     * @return batch query
     */
    public BatchQuery batchQuery() {
        return new BatchQuery(this);
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
    public BatchQuery batchQuery(int queryCount) {
        return new BatchQuery(this, queryCount);
    }

    /**
     * Creates a batch query with the given queries
     *
     * @param queries the queries in the batch
     * @return BatchQuery
     */
    public BatchQuery batchQuery(PqlQuery... queries) {
        return new BatchQuery(this, queries);
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
     *     Union performs a logical OR on the results of each BITMAP_CALL query passed to it.
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
     *     Intersect performs a logical AND on the results of each BITMAP_CALL query passed to it.
     *
     * @param bitmaps 2 or more bitmaps to intersect
     * @return a PQL query
     * @throws IllegalArgumentException if the number of bitmaps is less than 2
     * @see <a href="https://www.pilosa.com/docs/query-language/#intersect">Intersect Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery intersect(PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Intersect", bitmaps);
    }

    /**
     * Creates a Difference query.
     * <p>
     *     Difference returns all of the bits from the first BITMAP_CALL argument
     *     passed to it, without the bits from each subsequent BITMAP_CALL.
     *
     * @param bitmaps 2 or more bitmaps to differentiate
     * @return a PQL query
     * @throws IllegalArgumentException if the number of bitmaps is less than 2
     * @see <a href="https://www.pilosa.com/docs/query-language/#difference">Difference Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery difference(PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Difference", bitmaps);
    }

    /**
     * Creates a Count query.
     * <p>
     *     Returns the number of set bits in the BITMAP_CALL passed in.
     *
     * @param bitmap the bitmap query
     * @return a PQL query
     * @throws IllegalArgumentException if the number of bitmaps is less than 2
     * @see <a href="https://www.pilosa.com/docs/query-language/#count">Count Query</a>
     */
    public PqlBaseQuery count(PqlBitmapQuery bitmap) {
        return pqlQuery(String.format("Count(%s)", bitmap.serialize()));
    }

    /**
     * Creates a SetColumnAttrs query.
     * <p>
     *     SetColumnAttrs associates arbitrary key/value pairs with a column in an index.
     * <p>
     *     Following object types are accepted:
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

    PqlBaseQuery pqlQuery(String query) {
        return new PqlBaseQuery(query, this);
    }

    PqlBitmapQuery pqlBitmapQuery(String query) {
        return new PqlBitmapQuery(query, this);
    }

    private PqlBitmapQuery bitmapOperation(String name, PqlBitmapQuery... bitmaps) {
        if (bitmaps.length < 2) {
            throw new IllegalArgumentException(String.format("%s operation requires at least 2 bitmaps", name));
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
}
