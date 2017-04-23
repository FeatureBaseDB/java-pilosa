package com.pilosa.client.orm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.ValidationException;

import java.util.Map;

public class Index {
    private String name;
    private IndexOptions options;
    private ObjectMapper mapper = new ObjectMapper();

    private Index(String name, IndexOptions options) {
        this.name = name;
        this.options = options;
    }

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
     * Creates a index with a name and options
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

    /**
     * Gets the index name.
     *
     * @return index name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets options for this index.
     *
     * @return index options
     */
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
     * Creates a frame with the specified name and options
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
     * Creates a batch query with the given size.
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

    public PqlBaseQuery rawQuery(String query) {
        return new PqlBaseQuery(query, this);
    }

    /**
     * Creates a Union query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @param bitmaps other Bitmaps
     * @return a PQL query
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery union(PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Union", bitmap1, bitmap2, bitmaps);
    }

    /**
     * Creates an Intersect query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @param bitmaps other Bitmaps
     * @return a PQL query
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery intersect(PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Intersect", bitmap1, bitmap2, bitmaps);
    }

    /**
     * Creates a Difference query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @param bitmaps other Bitmaps
     * @return a PQL query
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery difference(PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Difference", bitmap1, bitmap2, bitmaps);
    }

    /**
     * Creates a Count query.
     *
     * @param bitmap the bitmap query
     * @return a PQL query
     */
    public PqlBaseQuery count(PqlBitmapQuery bitmap) {
        return pqlQuery(String.format("Count(%s)", bitmap.serialize()));
    }

    /**
     * Creates a SetProfileAttrs query
     *
     * @param id         coumn ID
     * @param attributes column attributes
     * @return a PQL query
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

    private PqlBitmapQuery bitmapOperation(String name, PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        String qry = String.format("%s, %s", bitmap1.serialize(), bitmap2.serialize());
        if (bitmaps.length > 0) {
            StringBuilder builder = new StringBuilder(bitmaps.length);
            builder.append(qry);
            for (PqlBitmapQuery bitmap : bitmaps) {
                builder.append(", ");
                builder.append(bitmap.serialize());
            }
            qry = builder.toString();
        }
        return pqlBitmapQuery(String.format("%s(%s)", name, qry));
    }
}
