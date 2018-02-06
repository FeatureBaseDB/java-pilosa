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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.exceptions.ValidationException;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Frames are used to segment and define different functional characteristics within your entire index.
 * <p>
 * You can think of a Frame as a table-like data partition within your Index.
 * Row-level attributes are namespaced at the Frame level.
 * <p>
 * Do not create a Frame object directly. Instead, use {@link com.pilosa.client.orm.Index#frame(String)} method.
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/">Data Model</a>
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public class Frame {
    private Frame(Index index, String name, FrameOptions options) {
        this.index = index;
        this.name = name;
        this.options = options;
        this.columnLabel = "columnID";
        this.fields = new HashMap<>();
    }

    /**
     * Creates a frame.
     *
     * @param index   the index this frame belongs to
     * @param name    name of the frame
     * @param options frame options
     * @return a Frame object
     * @throws ValidationException if an invalid frame name is passed
     */
    static Frame create(Index index, String name, FrameOptions options) {
        Validator.ensureValidFrameName(name);
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
     * <p>
     *     Bitmap retrieves the indices of all the set bits in a row or column
     *     based on whether the row label or column label is given in the query.
     *     It also retrieves any attributes set on that row or column.
     *
     * <p>
     *     This variant of Bitmap query uses the row label.
     *
     * @param rowID row ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#bitmap">Bitmap Query</a>
     */
    public PqlBitmapQuery bitmap(long rowID) {
        return this.index.pqlBitmapQuery(String.format("Bitmap(rowID=%d, frame='%s')", rowID, this.name));
    }

    /**
     * Creates a Bitmap query. (Enterprise version)
     * <p>
     * Bitmap retrieves the indices of all the set bits in a row or column
     * based on whether the row label or column label is given in the query.
     * It also retrieves any attributes set on that row or column.
     * <p>
     * <p>
     * This variant of Bitmap query uses the row label.
     *
     * @param rowKey row key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#bitmap">Bitmap Query</a>
     */
    public PqlBitmapQuery bitmap(String rowKey) {
        return this.index.pqlBitmapQuery(String.format("Bitmap(%s='%s', frame='%s')",
                this.rowLabel, rowKey, this.name));
    }

    /**
     * Creates a Bitmap query.
     * <p>
     *     Bitmap retrieves the indices of all the set bits in a row or column
     *     based on whether the row label or column label is given in the query.
     *     It also retrieves any attributes set on that row or column.
     *
     * <p>
     *     This variant of Bitmap query uses the column label.
     *
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#bitmap">Bitmap Query</a>
     */
    public PqlBitmapQuery inverseBitmap(long columnID) {
        return this.index.pqlBitmapQuery(String.format("Bitmap(%s=%d, frame='%s')",
                this.columnLabel, columnID, this.name));
    }

    /**
     * Creates a Bitmap query. (Enterprise version)
     * <p>
     *     Bitmap retrieves the indices of all the set bits in a row or column
     *     based on whether the row label or column label is given in the query.
     *     It also retrieves any attributes set on that row or column.
     *
     * <p>
     *     This variant of Bitmap query uses the column label.
     *
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#bitmap">Bitmap Query</a>
     */
    public PqlBitmapQuery inverseBitmap(String columnKey) {
        return this.index.pqlBitmapQuery(String.format("Bitmap(%s='%s', frame='%s')",
                this.columnLabel, columnKey, this.name));
    }

    /**
     * Creates a SetBit query.
     * <p>
     *  SetBit assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given frame with the given column.
     *
     * @param rowID    row ID
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery setBit(long rowID, long columnID) {
        return this.index.pqlQuery(String.format("SetBit(rowID=%d, frame='%s', %s=%d)",
                rowID, name, this.columnLabel, columnID));
    }

    /**
     * Creates a SetBit query. (Enterprise version)
     * <p>
     *  SetBit assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given frame with the given column.
     *
     * @param rowKey    row key
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery setBit(String rowKey, String columnKey) {
        return this.index.pqlQuery(String.format("SetBit(%s='%s', frame='%s', %s='%s')",
                this.rowLabel, rowKey, name, this.columnLabel, columnKey));
    }

    /**
     /**
     * Creates a SetBit query.
     * <p>
     *  SetBit, assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given frame with the given column.
     * <p>
     *      This variant supports providing a timestamp.
     *
     *
     * @param rowID     row ID
     * @param columnID  column ID
     * @param timestamp timestamp of the bit
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery setBit(long rowID, long columnID, Date timestamp) {
        String qry = String.format("SetBit(rowID=%d, frame='%s', %s=%d, timestamp='%sT%s')",
                rowID, name, this.columnLabel, columnID,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        return this.index.pqlQuery(qry);
    }

    /**
     /**
     * Creates a SetBit query. (Enterprise version)
     * <p>
     *  SetBit, assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given frame with the given column.
     * <p>
     *      This variant supports providing a timestamp.
     *
     *
     * @param rowKey     row key
     * @param columnKey  column key
     * @param timestamp timestamp of the bit
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery setBit(String rowKey, String columnKey, Date timestamp) {
        String qry = String.format("SetBit(%s='%s', frame='%s', %s='%s', timestamp='%sT%s')",
                this.rowLabel, rowKey, name, this.columnLabel, columnKey,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        return this.index.pqlQuery(qry);
    }

    /**
     * Creates a ClearBit query.
     * <p>
     *     ClearBit assigns a value of 0 to a bit in the binary matrix,
     *     thus disassociating the given row in the given frame from the given column.
     *
     * @param rowID    row ID
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clearBit(long rowID, long columnID) {
        return this.index.pqlQuery(String.format("ClearBit(rowID=%d, frame='%s', %s=%d)",
                rowID, name, this.columnLabel, columnID));
    }

    /**
     * Creates a ClearBit query. (Enterprise version)
     * <p>
     *     ClearBit assigns a value of 0 to a bit in the binary matrix,
     *     thus disassociating the given row in the given frame from the given column.
     *
     * @param rowKey    row key
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clearBit(String rowKey, String columnKey) {
        return this.index.pqlQuery(String.format("ClearBit(%s='%s', frame='%s', %s='%s')",
                this.rowLabel, rowKey, name, this.columnLabel, columnKey));
    }

    /**
     * Creates a TopN query.
     * <p>
     * TopN returns the id and count of the top n bitmaps (by count of bits) in the frame.
     *
     * @param n number of items to return
     * @return a PQL Bitmap query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    public PqlBaseQuery topN(long n) {
        String s = String.format("TopN(frame='%s', n=%d, inverse=false)", this.name, n);
        return this.index.pqlQuery(s);
    }

    /**
     * Creates a TopN query.
     * <p>
     * TopN returns the id and count of the top n bitmaps (by count of bits) in the frame.
     * This variant sets inverse=true
     *
     * @param n number of items to return
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    public PqlBaseQuery inverseTopN(long n) {
        String s = String.format("TopN(frame='%s', n=%d, inverse=true)", this.name, n);
        return this.index.pqlQuery(s);
    }

    /**
     * Creates a TopN query.
     * <p>
     * Return the id and count of the top n bitmaps (by count of bits) in the frame.
     * <p>
     * This variant supports customizing the bitmap query.
     *
     * @param n      number of items to return
     * @param bitmap the bitmap query
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery topN(long n, PqlBitmapQuery bitmap) {
        return this._topN(n, bitmap, false, null, null);
    }

    /**
     * Creates a TopN query.
     * <p>
     * Return the id and count of the top n bitmaps (by count of bits) in the frame.
     * <p>
     * This variant supports customizing the bitmap query and sets inverse=true
     *
     * @param n      number of items to return
     * @param bitmap the bitmap query
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery inverseTopN(long n, PqlBitmapQuery bitmap) {
        return this._topN(n, bitmap, true, null, null);
    }

    /**
     * Creates a TopN query.
     * <p>
     *     Return the id and count of the top n bitmaps (by count of bits) in the frame.
     *     The field and filters arguments work together to only return Bitmaps
     *     which have the attribute specified by field with one of the values specified
     *     in filters.
     *
     * @param n      number of items to return
     * @param bitmap the bitmap query
     * @param field  field name
     * @param values filter values to be matched against the field
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery topN(long n, PqlBitmapQuery bitmap, String field, Object... values) {
        return _topN(n, bitmap, false, field, values);
    }

    /**
     * Creates a TopN query.
     * <p>
     * Return the id and count of the top n bitmaps (by count of bits) in the frame.
     * The field and filters arguments work together to only return Bitmaps
     * which have the attribute specified by field with one of the values specified
     * in filters.
     * <p>
     * This variant supports customizing the bitmap query and sets inverse=true
     *
     * @param n      number of items to return
     * @param bitmap the bitmap query
     * @param field  field name
     * @param values filter values to be matched against the field
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery inverseTopN(long n, PqlBitmapQuery bitmap, String field, Object... values) {
        return _topN(n, bitmap, true, field, values);
    }

    private PqlBaseQuery _topN(long n, PqlBitmapQuery bitmap, boolean inverse, String field, Object[] values) {
        // TOOD: make field use its own validator
        String fieldString = "";
        if (field != null) {
            Validator.ensureValidLabel(field);
            fieldString = String.format(", field='%s'", field);
        }

        try {
            String valuesString = (values == null || values.length == 0) ? "" : String.format(", filters=%s", this.mapper.writeValueAsString(values));
            String inverseString = inverse ? "true" : "false";
            String bitmapString = (bitmap == null) ? "" : String.format("%s, ", bitmap.serialize());
            String s = String.format("TopN(%sframe='%s', n=%d, inverse=%s%s%s)",
                    bitmapString, this.name, n, inverseString, fieldString, valuesString);
            return this.index.pqlQuery(s);
        } catch (JsonProcessingException ex) {
            throw new PilosaException("Error while converting values", ex);
        }
    }

    /**
     * Creates a Range query.
     * <p>
     *     Similar to Bitmap, but only returns bits which were set with timestamps
     *     between the given start and end timestamps.
     *
     * @param rowID row ID
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#range">Range Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery range(long rowID, Date start, Date end) {
        return this.index.pqlBitmapQuery(String.format("Range(rowID=%d, frame='%s', start='%sT%s', end='%sT%s')",
                rowID, this.name, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a Range query. (Enterprise version)
     * <p>
     *     Similar to Bitmap, but only returns bits which were set with timestamps
     *     between the given start and end timestamps.
     *
     * @param rowKey row key
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#range">Range Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery range(String rowKey, Date start, Date end) {
        return this.index.pqlBitmapQuery(String.format("Range(%s='%s', frame='%s', start='%sT%s', end='%sT%s')",
                this.rowLabel, rowKey, this.name, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a Range query.
     * <p>
     * Similar to Bitmap, but only returns bits which were set with timestamps
     * between the given start and end timestamps.
     *
     * @param columnID bitmap ID
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#range">Range Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery inverseRange(long columnID, Date start, Date end) {
        return this.index.pqlBitmapQuery(String.format("Range(%s=%d, frame='%s', start='%sT%s', end='%sT%s')",
                this.columnLabel, columnID, this.name, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a Range query. (Enterprise version)
     * <p>
     * Similar to Bitmap, but only returns bits which were set with timestamps
     * between the given start and end timestamps.
     *
     * @param columnKey bitmap key
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#range">Range Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBitmapQuery inverseRange(String columnKey, Date start, Date end) {
        return this.index.pqlBitmapQuery(String.format("Range(%s='%s', frame='%s', start='%sT%s', end='%sT%s')",
                this.columnLabel, columnKey, this.name, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a SetRowAttrs query.
     * <p>
     *     SetRowAttrs associates arbitrary key/value pairs with a row in a frame.
     * <p>
     *     Following object types are accepted:
     *     <ul>
     *         <li>Long</li>
     *         <li>String</li>
     *         <li>Boolean</li>
     *         <li>Double</li>
     *     </ul>
     *
     * @param rowID      row ID
     * @param attributes row attributes
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setrowattrs">SetRowAttrs Query</a>
     */
    public PqlBaseQuery setRowAttrs(long rowID, Map<String, Object> attributes) {
        String attributesString = Util.createAttributesString(this.mapper, attributes);
        return this.index.pqlQuery(String.format("SetRowAttrs(rowID=%d, frame='%s', %s)",
                rowID, this.name, attributesString));
    }

    /**
     * Creates a SetRowAttrs query.
     * <p>
     *     SetRowAttrs associates arbitrary key/value pairs with a row in a frame.
     * <p>
     *     Following object types are accepted:
     *     <ul>
     *         <li>Long</li>
     *         <li>String</li>
     *         <li>Boolean</li>
     *         <li>Double</li>
     *     </ul>
     *
     * @param rowKey      row key
     * @param attributes row attributes
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setrowattrs">SetRowAttrs Query</a>
     */
    public PqlBaseQuery setRowAttrs(String rowKey, Map<String, Object> attributes) {
        String attributesString = Util.createAttributesString(this.mapper, attributes);
        return this.index.pqlQuery(String.format("SetRowAttrs(%s='%s', frame='%s', %s)",
                this.rowLabel, rowKey, this.name, attributesString));
    }

    /**
     * Returns a RangeField object with the given name
     * @param name field name
     * @return RangeField object
     */
    public RangeField field(String name) {
        RangeField field = this.fields.get(name);
        if (field == null) {
            Validator.ensureValidLabel(name);
            field = new RangeField(this, name);
            this.fields.put(name, field);
        }
        return field;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Frame)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Frame rhs = (Frame) obj;
        return rhs.index.getName().equals(this.index.getName()) &&
                rhs.options.equals(this.options);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.index.getName())
                .append(this.options)
                .toHashCode();
    }

    private final static DateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");
    private final static DateFormat fmtTime = new SimpleDateFormat("HH:mm");
    private String name;
    private Index index;
    private FrameOptions options;
    private String columnLabel;
    private Map<String, RangeField> fields;
    private ObjectMapper mapper = new ObjectMapper();
}
