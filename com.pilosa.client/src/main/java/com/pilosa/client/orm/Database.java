package com.pilosa.client.orm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.DatabaseOptions;
import com.pilosa.client.FrameOptions;
import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.ValidationException;

import java.util.Map;

public class Database {
    private String name;
    private DatabaseOptions options;
    private ObjectMapper mapper = new ObjectMapper();

    private Database(String name, DatabaseOptions options) {
        this.name = name;
        this.options = options;
    }

    /**
     * Create a database with a name using defaults.
     *
     * @param name database name
     * @return a Database object
     * @throws ValidationException if the passed database name is not valid
     */
    public static Database withName(String name) {
        return Database.withName(name, DatabaseOptions.withDefaults());
    }

    /**
     * Creates a database with a name and options
     *
     * @param name    database name
     * @param options database options
     * @return a Database object
     * @throws ValidationException if the passed database name is not valid
     */
    public static Database withName(String name, DatabaseOptions options) {
        Validator.ensureValidLabel(options.getColumnLabel());
        return new Database(name, options);
    }

    /**
     * Gets the database name.
     *
     * @return database name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Gets options for this database.
     *
     * @return database options
     */
    public DatabaseOptions getOptions() {
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
    public BatchQuery batchQuery(int queryCount) {
        return new BatchQuery(queryCount, this);
    }

    /**
     * Creates a Union query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @param bitmaps other Bitmaps
     * @return a PQL query
     */
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
        return pqlQuery(String.format("Count(%s)", bitmap));
    }

    /**
     * Creates a SetProfileAttrs query
     *
     * @param id         profile ID
     * @param attributes profile attributes
     * @return a PQL query
     */
    public PqlBaseQuery setProfileAttrs(long id, Map<String, Object> attributes) {
        String attributesString = Util.createAttributesString(this.mapper, attributes);
        return pqlQuery(String.format("SetProfileAttrs(%s=%d, %s)",
                this.options.getColumnLabel(), id, attributesString));
    }

    PqlBaseQuery pqlQuery(String query) {
        return new PqlBaseQuery(query, this);
    }

    PqlBitmapQuery pqlBitmapQuery(String query) {
        return new PqlBitmapQuery(query, this);
    }

    private PqlBitmapQuery bitmapOperation(String name, PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        String qry = String.format("%s, %s", bitmap1, bitmap2);
        if (bitmaps.length > 0) {
            StringBuilder builder = new StringBuilder(bitmaps.length);
            builder.append(qry);
            for (PqlBitmapQuery bitmap : bitmaps) {
                builder.append(", ");
                builder.append(bitmap);
            }
            qry = builder.toString();
        }
        return pqlBitmapQuery(String.format("%s(%s)", name, qry));
    }
}
