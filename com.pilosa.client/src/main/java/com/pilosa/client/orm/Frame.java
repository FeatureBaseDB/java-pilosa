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

/**
 * Frames are used to segment and define different functional characteristics within your entire index.
 * <p>
 * You can think of a Frame as a table-like data partition within your Index.
 * Row-level attributes are namespaced at the Frame level.
 * <p>
 * <p>
 * Use <code>Index.frame</code> method to create a <code>Frame</code> object.
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/#frame">Pilosa Data Model: Frame</a>
 * @see com.pilosa.client.orm.Index#frame(String) Index.frame
 * </p>
 */
public class Frame {
    private Frame(Index index, String name, FrameOptions options) {
        this.index = index;
        this.name = name;
        this.options = options;
        this.columnLabel = index.getOptions().getColumnLabel();
        this.rowLabel = options.getRowLabel();
    }

    /**
     * Creates a frame.
     *
     * @param index   the index this frame belongs to
     * @param name    name of the frame
     * @param options frame options or <code>FrameOptions.withDefaults()</code>
     * @return a Frame object
     * @throws ValidationException if an invalid frame name is passed
     */
    static Frame create(Index index, String name, FrameOptions options) {
        Validator.ensureValidFrameName(name);
        Validator.ensureValidLabel(options.getRowLabel());
        return new Frame(index, name, options);
    }

    public Index getIndex() {
        return this.index;
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
     * @param rowID row ID
     * @return a PQL query
     */
    public PqlBitmapQuery bitmap(long rowID) {
        return this.index.pqlBitmapQuery(String.format("Bitmap(%s=%d, frame='%s')",
                this.rowLabel, rowID, this.name));
    }

    /**
     * Creates an inverse Bitmap query
     *
     * @param columnID column ID
     * @return a PQL query
     */
    public PqlBaseQuery inverseBitmap(long columnID) {
        if (!this.options.isInverseEnabled()) {
            throw new PilosaException("Inverse bitmaps support was not enabled for this frame");
        }
        return this.index.pqlQuery(String.format("Bitmap(%s=%d, frame='%s')",
                this.columnLabel, columnID, this.name));
    }

    /**
     * Creates a SetBit query
     *
     * @param rowID    bitmap ID
     * @param columnID column ID
     * @return a PQL query
     */
    public PqlBaseQuery setBit(long rowID, long columnID) {
        return this.index.pqlQuery(String.format("SetBit(%s=%d, frame='%s', %s=%d)",
                this.rowLabel, rowID, name, this.columnLabel, columnID));
    }

    /**
     * Creates a SetBit query
     *
     * @param rowID     bitmap ID
     * @param columnID  column ID
     * @param timestamp timestamp of the bit
     * @return a PQL query
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery setBit(long rowID, long columnID, Date timestamp) {
        String qry = String.format("SetBit(%s=%d, frame='%s', %s=%d, timestamp='%sT%s')",
                this.rowLabel, rowID, name, this.columnLabel, columnID,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        return this.index.pqlQuery(qry);
    }

    /**
     * Creates a ClearBit query
     *
     * @param rowID    bitmap ID
     * @param columnID column ID
     * @return a PQL query
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clearBit(long rowID, long columnID) {
        return this.index.pqlQuery(String.format("ClearBit(%s=%d, frame='%s', %s=%d)",
                this.rowLabel, rowID, name, this.columnLabel, columnID));
    }

    /**
     * Creates a TopN query.
     *
     * @param n number of items to return
     * @return a PQL Bitmap query
     */
    public PqlBitmapQuery topN(long n) {
        String s = String.format("TopN(frame='%s', n=%d)", this.name, n);
        return this.index.pqlBitmapQuery(s);
    }

    /**
     * Creates a TopN query.
     *
     * @param n      number of items to return
     * @param bitmap the bitmap query
     * @return a PQL query
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap) {
        String s = String.format("TopN(%s, frame='%s', n=%d)",
                bitmap.serialize(), this.name, n);
        return this.index.pqlBitmapQuery(s);
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
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap, String field, Object... values) {
        // TOOD: make field use its own validator
        Validator.ensureValidLabel(field);
        try {
            String valuesString = this.mapper.writeValueAsString(values);
            String s = String.format("TopN(%s, frame='%s', n=%d, field='%s', %s)",
                    bitmap.serialize(), this.name, n, field, valuesString);
            return this.index.pqlBitmapQuery(s);
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
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery range(long rowID, Date start, Date end) {
        return this.index.pqlBitmapQuery(String.format("Range(%s=%d, frame='%s', start='%sT%s', end='%sT%s')",
                this.rowLabel, rowID, this.name, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a SetRowAttrs query.
     *
     * @param rowID      row ID
     * @param attributes row attributes
     * @return a PQL query
     */
    public PqlBaseQuery setRowAttrs(long rowID, Map<String, Object> attributes) {
        String attributesString = Util.createAttributesString(this.mapper, attributes);
        return this.index.pqlQuery(String.format("SetRowAttrs(%s=%d, frame='%s', %s)",
                this.rowLabel, rowID, this.name, attributesString));
    }

    private final static DateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");
    private final static DateFormat fmtTime = new SimpleDateFormat("HH:mm");
    private String name;
    private Index index;
    private FrameOptions options;
    private String rowLabel;
    private String columnLabel;
    private ObjectMapper mapper = new ObjectMapper();
}
