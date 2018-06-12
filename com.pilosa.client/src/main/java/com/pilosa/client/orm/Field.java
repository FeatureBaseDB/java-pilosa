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
import java.util.Map;

/**
 * Frames are used to segment and define different functional characteristics within your entire index.
 * <p>
 * You can think of a Field as a table-like data partition within your Index.
 * Row-level attributes are namespaced at the Field level.
 * <p>
 * Do not create a Field object directly. Instead, use {@link com.pilosa.client.orm.Index#field(String)} method.
 *
 * @see <a href="https://www.pilosa.com/docs/data-model/">Data Model</a>
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public class Field {
    private Field(Index index, String name, FieldOptions options) {
        this.index = index;
        this.name = name;
        this.options = options;
    }

    /**
     * Creates a field.
     *
     * @param index   the index this field belongs to
     * @param name    name of the field
     * @param options field options
     * @return a Field object
     * @throws ValidationException if an invalid field name is passed
     */
    static Field create(Index index, String name, FieldOptions options) {
        Validator.ensureValidFrameName(name);
        return new Field(index, name, options);
    }

    public Index getIndex() {
        return this.index;
    }

    public String getName() {
        return this.name;
    }

    public FieldOptions getOptions() {
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
        return this.index.pqlBitmapQuery(String.format("Bitmap(row=%d, field='%s')", rowID, this.name));
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
        return this.index.pqlBitmapQuery(String.format("Bitmap(row='%s', field='%s')",
                rowKey, this.name));
    }

    /**
     * Creates a SetBit query.
     * <p>
     *  SetBit assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given field with the given column.
     *
     * @param rowID    row ID
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery setBit(long rowID, long columnID) {
        return this.index.pqlQuery(String.format("SetBit(row=%d, field='%s', col=%d)",
                rowID, name, columnID));
    }

    /**
     * Creates a SetBit query. (Enterprise version)
     * <p>
     *  SetBit assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given field with the given column.
     *
     * @param rowKey    row key
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery setBit(String rowKey, String columnKey) {
        return this.index.pqlQuery(String.format("SetBit(row='%s', field='%s', col='%s')",
                rowKey, name, columnKey));
    }

    /**
     /**
     * Creates a SetBit query.
     * <p>
     *  SetBit, assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given field with the given column.
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
        String qry = String.format("SetBit(row=%d, field='%s', col=%d, timestamp='%sT%s')",
                rowID, name, columnID,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        return this.index.pqlQuery(qry);
    }

    /**
     /**
     * Creates a SetBit query. (Enterprise version)
     * <p>
     *  SetBit, assigns a value of 1 to a bit in the binary matrix,
     *  thus associating the given row in the given field with the given column.
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
        String qry = String.format("SetBit(row='%s', field='%s', col='%s', timestamp='%sT%s')",
                rowKey, name, columnKey,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        return this.index.pqlQuery(qry);
    }

    /**
     * Creates a ClearBit query.
     * <p>
     *     ClearBit assigns a value of 0 to a bit in the binary matrix,
     *     thus disassociating the given row in the given field from the given column.
     *
     * @param rowID    row ID
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clearBit(long rowID, long columnID) {
        return this.index.pqlQuery(String.format("ClearBit(row=%d, field='%s', col=%d)",
                rowID, name, columnID));
    }

    /**
     * Creates a ClearBit query. (Enterprise version)
     * <p>
     *     ClearBit assigns a value of 0 to a bit in the binary matrix,
     *     thus disassociating the given row in the given field from the given column.
     *
     * @param rowKey    row key
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clearBit(String rowKey, String columnKey) {
        return this.index.pqlQuery(String.format("ClearBit(row='%s', field='%s', col='%s')",
                rowKey, name, columnKey));
    }

    /**
     * Creates a TopN query.
     * <p>
     * TopN returns the id and count of the top n bitmaps (by count of bits) in the field.
     *
     * @param n number of items to return
     * @return a PQL Bitmap query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    public PqlBaseQuery topN(long n) {
        String s = String.format("TopN(field='%s', n=%d)", this.name, n);
        return this.index.pqlQuery(s);
    }

    /**
     * Creates a TopN query.
     * <p>
     * Return the id and count of the top n bitmaps (by count of bits) in the field.
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
        return this._topN(n, bitmap, null, null);
    }

    /**
     * Creates a TopN query.
     * <p>
     *     Return the id and count of the top n bitmaps (by count of bits) in the field.
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
        return _topN(n, bitmap, field, values);
    }

    private PqlBaseQuery _topN(long n, PqlBitmapQuery bitmap, String field, Object[] values) {
        // TOOD: make field use its own validator
        String fieldString = "";
        if (field != null) {
            Validator.ensureValidLabel(field);
            fieldString = String.format(", field='%s'", field);
        }

        try {
            String valuesString = (values == null || values.length == 0) ? "" : String.format(", filters=%s", this.mapper.writeValueAsString(values));
            String bitmapString = (bitmap == null) ? "" : String.format("%s, ", bitmap.serialize());
            String s = String.format("TopN(%sfield='%s', n=%d%s%s)",
                    bitmapString, this.name, n, fieldString, valuesString);
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
        return this.index.pqlBitmapQuery(String.format("Range(row=%d, field='%s', start='%sT%s', end='%sT%s')",
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
        return this.index.pqlBitmapQuery(String.format("Range(row='%s', field='%s', start='%sT%s', end='%sT%s')",
                rowKey, this.name, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a SetRowAttrs query.
     * <p>
     *     SetRowAttrs associates arbitrary key/value pairs with a row in a field.
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
        return this.index.pqlQuery(String.format("SetRowAttrs(row=%d, field='%s', %s)",
                rowID, this.name, attributesString));
    }

    /**
     * Creates a SetRowAttrs query.
     * <p>
     *     SetRowAttrs associates arbitrary key/value pairs with a row in a field.
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
        return this.index.pqlQuery(String.format("SetRowAttrs(row='%s', field='%s', %s)",
                rowKey, this.name, attributesString));
    }

    /**
     * Creates a Range query with less than (<) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlBitmapQuery lessThan(long n) {
        return binaryOperation("<", n);
    }

    /**
     * Creates a Range query with less than or equal (<=) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlBitmapQuery lessThanOrEqual(long n) {
        return binaryOperation("<=", n);
    }

    /**
     * Creates a Range query with greater than (>) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlBitmapQuery greaterThan(long n) {
        return binaryOperation(">", n);
    }

    /**
     * Creates a Range query with greater than or equal (>=) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlBitmapQuery greaterThanOrEqual(long n) {
        return binaryOperation(">=", n);
    }

    /**
     * Creates a Range query with equals (==) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlBitmapQuery equals(long n) {
        return binaryOperation("==", n);
    }

    /**
     * Creates a Range query with not equal to (!=) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlBitmapQuery notEquals(long n) {
        return binaryOperation("!=", n);
    }

    /**
     * Creates a Range query with not null (!= null) condition.
     *
     * @return a PQL query
     */
    public PqlBitmapQuery notNull() {
        String qry = String.format("Range(%s != null)", this.name);
        return this.index.pqlBitmapQuery(qry);
    }

    /**
     * Creates a Range query with between (><) condition.
     *
     * @param a Closed range start
     * @param b Closed range end
     * @return a PQL query
     */
    public PqlBitmapQuery between(long a, long b) {
        String qry = String.format("Range(%s >< [%d,%d])", this.name, a, b);
        return this.index.pqlBitmapQuery(qry);
    }

    /**
     * Creates a Sum query.
     * <p>
     * The field for this query should have fields set.
     * </p>
     *
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#sum">Sum Query</a>
     */
    public PqlBaseQuery sum() {
        return valueQuery("Sum", null);
    }

    /**
     * Creates a Sum query.
     * <p>
     * The field for this query should have fields set.
     * </p>
     *
     * @param bitmap The bitmap query to use.
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#sum">Sum Query</a>
     */
    public PqlBaseQuery sum(PqlBitmapQuery bitmap) {
        return valueQuery("Sum", bitmap);
    }

    /**
     * Creates a Min query.
     * <p>
     * The field for this query should have fields set.
     * </p>
     *
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#min">Min Query</a>
     */
    public PqlBaseQuery min() {
        return valueQuery("Min", null);
    }

    /**
     * Creates a Min query.
     * <p>
     * The field for this query should have fields set.
     * </p>
     *
     * @param bitmap The bitmap query to use.
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#min">Min Query</a>
     */
    public PqlBaseQuery min(PqlBitmapQuery bitmap) {
        return valueQuery("Min", bitmap);
    }

    /**
     * Creates a Max query.
     * <p>
     * The field for this query should have fields set.
     * </p>
     *
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#max">Max Query</a>
     */
    public PqlBaseQuery max() {
        return valueQuery("Max", null);
    }

    /**
     * Creates a Max query.
     * <p>
     * The field for this query should have fields set.
     * </p>
     *
     * @param bitmap The bitmap query to use.
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#max">Max Query</a>
     */
    public PqlBaseQuery max(PqlBitmapQuery bitmap) {
        return valueQuery("Max", bitmap);
    }

    /**
     * Creates a SetFieldValue query.
     *
     * @param columnID column ID
     * @param value    the value to assign to the field
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setfieldvalue">SetFieldValue Query</a>
     */
    public PqlBaseQuery setValue(long columnID, long value) {
        String qry = String.format("SetValue(col=%d, %s=%d)",
                columnID, this.name, value);
        return this.index.pqlQuery(qry);
    }

    private PqlBitmapQuery binaryOperation(String op, long n) {
        String qry = String.format("Range(%s %s %d)", this.name, op, n);
        return this.index.pqlBitmapQuery(qry);
    }

    private PqlBaseQuery valueQuery(String op, PqlBitmapQuery bitmap) {
        String qry;
        if (bitmap != null) {
            qry = String.format("%s(%s, field='%s')",
                    op, bitmap.serialize(), this.name);
        } else {
            qry = String.format("%s(field='%s')", op, this.name);
        }
        return this.index.pqlQuery(qry);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Field)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Field rhs = (Field) obj;
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
    private FieldOptions options;
    private ObjectMapper mapper = new ObjectMapper();
}
