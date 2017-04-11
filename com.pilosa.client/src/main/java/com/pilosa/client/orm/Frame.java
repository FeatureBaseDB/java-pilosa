package com.pilosa.client.orm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.exceptions.ValidationException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Frame {
    private String name;
    private Database database;
    private FrameOptions options;
    private String rowLabel;
    private String columnLabel;
    private ObjectMapper mapper = new ObjectMapper();

    private Frame(Database database, String name, FrameOptions options) {
        this.database = database;
        this.name = name;
        this.options = options;
        this.columnLabel = database.getOptions().getColumnLabel();
        this.rowLabel = options.getRowLabel();
    }

    /**
     * Creates a frame.
     *
     * @param db      the database this frame belongs to
     * @param name    name of the frame
     * @param options frame options or <code>FrameOptions.withDefaults()</code>
     * @return a Frame object
     * @throws ValidationException if an invalid frame name is passed
     */
    static Frame create(Database db, String name, FrameOptions options) {
        Validator.ensureValidLabel(options.getRowLabel());
        return new Frame(db, name, options);
    }

    public Database getDatabase() {
        return this.database;
    }

    public String getName() {
        return this.name;
    }

    public FrameOptions getOptions() {
        return this.options;
    }

    /**
     * Creates a Bitmap query.
     *
     * @param rowID bitmap ID
     * @return a PQL query
     */
    public PqlBitmapQuery bitmap(long rowID) {
        return this.database.pqlBitmapQuery(String.format("Bitmap(%s=%d, frame='%s')",
                this.rowLabel, rowID, this.name));
    }

    /**
     * Creates a SetBit query
     *
     * @param rowID    bitmap ID
     * @param columnID profile ID
     * @return a PQL query
     */
    public PqlBaseQuery setBit(long rowID, long columnID) {
        return this.database.pqlQuery(String.format("SetBit(%s=%d, frame='%s', %s=%d)",
                this.rowLabel, rowID, name, this.columnLabel, columnID));
    }

    /**
     * Creates a ClearBit query
     *
     * @param rowID    bitmap ID
     * @param columnID profile ID
     * @return a PQL query
     */
    public PqlBaseQuery clearBit(long rowID, long columnID) {
        return this.database.pqlQuery(String.format("ClearBit(%s=%d, frame='%s', %s=%d)",
                this.rowLabel, rowID, name, this.columnLabel, columnID));
    }

    /**
     * Creates a TopN query.
     *
     * @param n number of items to return
     * @return a PQL Bitmap query
     */
    public PqlBitmapQuery topN(long n) {
        return this.database.pqlBitmapQuery(String.format("TopN(frame='%s', n=%d)", this.name, n));
    }

    /**
     * Creates a TopN query.
     *
     * @param n      number of items to return
     * @param bitmap the bitmap query
     * @return a PQL query
     */
    public PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap) {
        return this.database.pqlBitmapQuery(String.format("TopN(%s, frame='%s', n=%d)", bitmap, this.name, n));
    }

    /**
     * Creates a TopN query.
     *
     * @param n      number of items to return
     * @param bitmap the bitmap query
     * @param field  field name
     * @param values filter values to be matched against the field
     * @return a PQL query
     */
    public PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap, String field, Object... values) {
        // TOOD: make field use its own validator
        Validator.ensureValidLabel(field);
        try {
            String valuesString = this.mapper.writeValueAsString(values);
            return this.database.pqlBitmapQuery(String.format("TopN(%s, frame='%s', n=%d, field='%s', %s)",
                    bitmap, this.name, n, field, valuesString));
        } catch (JsonProcessingException ex) {
            throw new PilosaException("Error while converting values", ex);
        }
    }

    /**
     * Creates a Range query.
     *
     * @param rowID bitmap ID
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     */
    public PqlBitmapQuery range(long rowID, Date start, Date end) {
        DateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat fmtTime = new SimpleDateFormat("HH:mm");
        return this.database.pqlBitmapQuery(String.format("Range(%s=%d, frame='%s', start='%sT%s', end='%sT%s')",
                this.rowLabel, rowID, this.name, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a SetBitmapAttrs query.
     *
     * @param rowID      bitmap ID
     * @param attributes bitmap attributes
     * @return a PQL query
     */
    public PqlBaseQuery setBitmapAttrs(long rowID, Map<String, Object> attributes) {
        String attributesString = Util.createAttributesString(this.mapper, attributes);
        return this.database.pqlQuery(String.format("SetBitmapAttrs(%s=%d, frame='%s', %s)",
                this.rowLabel, rowID, this.name, attributesString));
    }
}
