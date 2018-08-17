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
 * Fields are used to segment and define different functional characteristics within your entire index.
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
     * Creates a Row query.
     * <p>
     *     Row retrieves the indices of all columns in a row
     *     based on whether the row label or column label is given in the query.
     *     It also retrieves any attributes set on that row or column.
     *
     * <p>
     *
     * @param rowID row ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#row">Row Query</a>
     */
    public PqlRowQuery row(long rowID) {
        return this.index.pqlRowQuery(String.format("Row(%s=%d)", this.name, rowID), false);
    }

    /**
     * Creates a Row query. (Enterprise version)
     * <p>
     *     Row retrieves the indices of all columns in a row
     *     based on whether the row label or column label is given in the query.
     *     It also retrieves any attributes set on that row or column.
     *
     * @param rowKey row key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#row">Row Query</a>
     */
    public PqlRowQuery row(String rowKey) {
        return this.index.pqlRowQuery(String.format("Row(%s='%s')",
                this.name, rowKey), false);
    }

    /**
     * Creates a Set query.
     * <p>
     *  Set assigns a value of 1 to a column in the binary matrix,
     *  thus associating the given row in the given field with the given column.
     *
     * @param rowID    row ID
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery set(long rowID, long columnID) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Set(%d,%s=%d)",
                columnID, name, rowID), hasKeys);
    }

    /**
     * Creates a Set query.
     * <p>
     * Set assigns a value of 1 to a column in the binary matrix,
     * thus associating the given row in the given field with the given column.
     *
     * @param rowID     row ID
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery set(long rowID, String columnKey) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Set('%s',%s=%d)",
                columnKey, name, rowID), hasKeys);
    }

    /**
     * Creates a Set query.
     * <p>
     * Set assigns a value of 1 to a column in the binary matrix,
     * thus associating the given row in the given field with the given column.
     *
     * @param rowKey   row key
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery set(String rowKey, long columnID) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Set(%d,%s='%s')",
                columnID, name, rowKey), hasKeys);
    }

    /**
     * Creates a Set query.
     * <p>
     *  Set assigns a value of 1 to a column in the binary matrix,
     *  thus associating the given row in the given field with the given column.
     *
     * @param rowKey    row key
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    public PqlBaseQuery set(String rowKey, String columnKey) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Set('%s',%s='%s')",
                columnKey, name, rowKey), hasKeys);
    }

     /**
     * Creates a Set query.
     * <p>
     *  Set, assigns a value of 1 to a column in the binary matrix,
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
    public PqlBaseQuery set(long rowID, long columnID, Date timestamp) {
        String qry = String.format("Set(%d,%s=%d,%sT%s)",
                columnID, name, rowID,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(qry, hasKeys);
    }

    /**
     * Creates a Set query.
     * <p>
     * Set, assigns a value of 1 to a column in the binary matrix,
     * thus associating the given row in the given field with the given column.
     * <p>
     * This variant supports providing a timestamp.
     *
     * @param rowID     row ID
     * @param columnKey column key
     * @param timestamp timestamp of the bit
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery set(long rowID, String columnKey, Date timestamp) {
        String qry = String.format("Set('%s',%s=%d,%sT%s)",
                columnKey, name, rowID,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(qry, hasKeys);
    }

    /**
     * Creates a Set query.
     * <p>
     * Set, assigns a value of 1 to a column in the binary matrix,
     * thus associating the given row in the given field with the given column.
     * <p>
     * This variant supports providing a timestamp.
     *
     * @param rowKey    row key
     * @param columnID  column ID
     * @param timestamp timestamp of the bit
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setbit">SetBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery set(String rowKey, long columnID, Date timestamp) {
        String qry = String.format("Set(%d,%s='%s',%sT%s)",
                columnID, name, rowKey,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(qry, hasKeys);
    }

    /**
     * Creates a Set query.
     * <p>
     *  Set, assigns a value of 1 to a column in the binary matrix,
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
    public PqlBaseQuery set(String rowKey, String columnKey, Date timestamp) {
        String qry = String.format("Set('%s',%s='%s',%sT%s)",
                columnKey, name, rowKey,
                fmtDate.format(timestamp), fmtTime.format(timestamp));
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(qry, hasKeys);
    }

    /**
     * Creates a Clear query.
     * <p>
     *     Clear assigns a value of 0 to a column in the binary matrix,
     *     thus disassociating the given row in the given field from the given column.
     *
     * @param rowID    row ID
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clear(long rowID, long columnID) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Clear(%d,%s=%d)",
                columnID, name, rowID), hasKeys);
    }

    /**
     * Creates a Clear query.
     * <p>
     *     Clear assigns a value of 0 to a column in the binary matrix,
     *     thus disassociating the given row in the given field from the given column.
     *
     * @param rowID    row ID
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clear(long rowID, String columnKey) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Clear('%s',%s=%d)",
                columnKey, name, rowID), hasKeys);
    }

    /**
     * Creates a Clear query.
     * <p>
     * Clear assigns a value of 0 to a column in the binary matrix,
     * thus disassociating the given row in the given field from the given column.
     *
     * @param rowKey   row key
     * @param columnID column ID
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clear(String rowKey, long columnID) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Clear(%d,%s='%s')",
                columnID, name, rowKey), hasKeys);
    }

    /**
     * Creates a Clear query.
     * <p>
     *     Clear assigns a value of 0 to a column in the binary matrix,
     *     thus disassociating the given row in the given field from the given column.
     *
     * @param rowKey    row key
     * @param columnKey column key
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#clearbit">ClearBit Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery clear(String rowKey, String columnKey) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        return this.index.pqlQuery(String.format("Clear('%s',%s='%s')",
                columnKey, name, rowKey), hasKeys);
    }

    /**
     * Creates a TopN query.
     * <p>
     * TopN returns the id and count of the top n rows (by count of bits) in the field.
     *
     * @param n number of items to return
     * @return a PQL Row query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    public PqlBaseQuery topN(long n) {
        String s = String.format("TopN(%s,n=%d)", this.name, n);
        return this.index.pqlQuery(s, false);
    }

    /**
     * Creates a TopN query.
     * <p>
     * Return the id and count of the top n rows (by count of bits) in the field.
     * <p>
     * This variant supports customizing the row query.
     *
     * @param n      number of items to return
     * @param row the row query
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery topN(long n, PqlRowQuery row) {
        return this._topN(n, row, null, null);
    }

    /**
     * Creates a TopN query.
     * <p>
     *     Return the id and count of the top n rows (by count of bits) in the field.
     *     The field and filters arguments work together to only return rows
     *     which have the attribute specified by field with one of the values specified
     *     in filters.
     *
     * @param n      number of items to return
     * @param row the row query
     * @param field  field name
     * @param values filter values to be matched against the field
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#topn">TopN Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlBaseQuery topN(long n, PqlRowQuery row, String field, Object... values) {
        return _topN(n, row, field, values);
    }

    private PqlBaseQuery _topN(long n, PqlRowQuery row, String field, Object[] values) {
        // TOOD: make field use its own validator
        String fieldString = "";
        if (field != null) {
            Validator.ensureValidLabel(field);
            fieldString = String.format(",field='%s'", field);
        }

        try {
            String valuesString = (values == null || values.length == 0) ? "" : String.format(",filters=%s", this.mapper.writeValueAsString(values));
            String rowString = (row == null) ? "" : String.format(",%s", row.serialize().getQuery());
            String s = String.format("TopN(%s%s,n=%d%s%s)",
                    this.name, rowString, n, fieldString, valuesString);
            return this.index.pqlQuery(s, false);
        } catch (JsonProcessingException ex) {
            throw new PilosaException("Error while converting values", ex);
        }
    }

    /**
     * Creates a Range query.
     * <p>
     *     Similar to Row, but only returns bits which were set with timestamps
     *     between the given start and end timestamps.
     *
     * @param rowID row ID
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#range">Range Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlRowQuery range(long rowID, Date start, Date end) {
        String text = String.format("Range(%s=%d,%sT%s,%sT%s)",
                this.name, rowID, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end));
        return this.index.pqlRowQuery(text, false);
    }

    /**
     * Creates a Range query. (Enterprise version)
     * <p>
     *     Similar to Row, but only returns bits which were set with timestamps
     *     between the given start and end timestamps.
     *
     * @param rowKey row key
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#range">Range Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlRowQuery range(String rowKey, Date start, Date end) {
        String text = String.format("Range(%s='%s',%sT%s,%sT%s)",
                this.name, rowKey, fmtDate.format(start),
                fmtTime.format(start), fmtDate.format(end), fmtTime.format(end));
        return this.index.pqlRowQuery(text, false);
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
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        String text = String.format("SetRowAttrs(%s,%d,%s)", this.name, rowID, attributesString);
        return this.index.pqlQuery(text, hasKeys);
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
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        String text = String.format("SetRowAttrs('%s','%s',%s)", this.name, rowKey, attributesString);
        return this.index.pqlQuery(text, hasKeys);
    }

    /**
     * Creates a Range query with less than (<) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlRowQuery lessThan(long n) {
        return binaryOperation("<", n);
    }

    /**
     * Creates a Range query with less than or equal (<=) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlRowQuery lessThanOrEqual(long n) {
        return binaryOperation("<=", n);
    }

    /**
     * Creates a Range query with greater than (>) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlRowQuery greaterThan(long n) {
        return binaryOperation(">", n);
    }

    /**
     * Creates a Range query with greater than or equal (>=) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlRowQuery greaterThanOrEqual(long n) {
        return binaryOperation(">=", n);
    }

    /**
     * Creates a Range query with equals (==) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlRowQuery equals(long n) {
        return binaryOperation("==", n);
    }

    /**
     * Creates a Range query with not equal to (!=) condition.
     *
     * @param n The value to compare
     * @return a PQL query
     */
    public PqlRowQuery notEquals(long n) {
        return binaryOperation("!=", n);
    }

    /**
     * Creates a Range query with not null (!= null) condition.
     *
     * @return a PQL query
     */
    public PqlRowQuery notNull() {
        String qry = String.format("Range(%s != null)", this.name);
        return this.index.pqlRowQuery(qry, false);
    }

    /**
     * Creates a Range query with between (><) condition.
     *
     * @param a Closed range start
     * @param b Closed range end
     * @return a PQL query
     */
    public PqlRowQuery between(long a, long b) {
        String qry = String.format("Range(%s >< [%d,%d])", this.name, a, b);
        return this.index.pqlRowQuery(qry, false);
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
     * @param row The row query to use.
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#sum">Sum Query</a>
     */
    public PqlBaseQuery sum(PqlRowQuery row) {
        return valueQuery("Sum", row);
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
     * @param row The row query to use.
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#min">Min Query</a>
     */
    public PqlBaseQuery min(PqlRowQuery row) {
        return valueQuery("Min", row);
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
     * @param row The row query to use.
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#max">Max Query</a>
     */
    public PqlBaseQuery max(PqlRowQuery row) {
        return valueQuery("Max", row);
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
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        String qry = String.format("Set(%d, %s=%d)",
                columnID, this.name, value);
        return this.index.pqlQuery(qry, hasKeys);
    }

    /**
     * Creates a SetFieldValue query.
     *
     * @param columnKey column key
     * @param value     the value to assign to the field
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setfieldvalue">SetFieldValue Query</a>
     */
    public PqlBaseQuery setValue(String columnKey, long value) {
        boolean hasKeys = this.index.getOptions().isKeys() || this.getOptions().isKeys();
        String qry = String.format("Set('%s', %s=%d)",
                columnKey, this.name, value);
        return this.index.pqlQuery(qry, hasKeys);
    }

    private PqlRowQuery binaryOperation(String op, long n) {
        String qry = String.format("Range(%s %s %d)", this.name, op, n);
        return this.index.pqlRowQuery(qry, false);
    }

    private PqlBaseQuery valueQuery(String op, PqlRowQuery row) {
        String qry;
        if (row != null) {
            qry = String.format("%s(%s,field='%s')",
                    op, row.serialize().getQuery(), this.name);
        } else {
            qry = String.format("%s(field='%s')", op, this.name);
        }
        return this.index.pqlQuery(qry, false);
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
        Validator.ensureValidFieldName(name);
        return new Field(index, name, options);
    }

    private Field(Index index, String name, FieldOptions options) {
        this.index = index;
        this.name = name;
        this.options = options;
    }

    private final static DateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");
    private final static DateFormat fmtTime = new SimpleDateFormat("HH:mm");
    private String name;
    private Index index;
    private FieldOptions options;
    private ObjectMapper mapper = new ObjectMapper();
}
