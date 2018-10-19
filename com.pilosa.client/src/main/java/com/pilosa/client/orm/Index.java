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
     * Creates an index with a name.
     *
     * @param name    index name
     * @return a Index object
     * @throws ValidationException if the passed index name is not valid
     */
    public static Index create(String name) {
        return create(name, IndexOptions.withDefaults());
    }

    /**
     * Creates an index with a name and options.
     *
     * @param name    index name
     * @param options index options
     * @return a Index object
     * @throws ValidationException if the passed index name is not valid
     */
    public static Index create(String name, IndexOptions options) {
        Validator.ensureValidIndexName(name);
        return new Index(name, options);

    }

    public String getName() {
        return this.name;
    }

    /**
     * Returns a field object with the specified name and defaults.
     *
     * @param name field name
     * @return a Field object
     * @throws ValidationException if the passed field name is not valid
     */
    public Field field(String name) {
        return this.field(name, FieldOptions.withDefaults());
    }

    /**
     * Returns a field with the specified name and options.
     *
     * @param name    field name
     * @param options field options
     * @return a Field object
     * @throws ValidationException if the passed field name is not valid
     */
    public Field field(String name, FieldOptions options) {
        if (this.fields.containsKey(name)) {
            return this.fields.get(name);
        }
        Field field = Field.create(this, name, options);
        this.fields.put(name, field);
        return field;
    }

    /**
     * Copies other field to this index and returns the new field
     *
     * @param other field
     * @return copied field
     */
    public Field field(Field other) {
        return field(other.getName(), other.getOptions());
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
        PqlBaseQuery q = new PqlBaseQuery(query, this);
        // raw queries are always assumed to have keys
        q.query.setWriteKeys(true);
        return q;
    }

    /**
     * Creates a Union query.
     * <p>
     * Union performs a logical OR on the results of each ROW_CALL query passed to it.
     *
     * @param rows 2 or more rows to union
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#union">Union Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlRowQuery union(PqlRowQuery... rows) {
        return rowOperation("Union", rows);
    }

    /**
     * Creates an Intersect query.
     * <p>
     * Intersect performs a logical AND on the results of each ROW_CALL query passed to it.
     *
     * @param rows 2 or more rows to intersect
     * @return a PQL query
     * @throws IllegalArgumentException if the number of rows is less than 1
     * @see <a href="https://www.pilosa.com/docs/query-language/#intersect">Intersect Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlRowQuery intersect(PqlRowQuery... rows) {
        if (rows.length < 1) {
            throw new IllegalArgumentException("Intersect operation requires at least 1 row");
        }
        return rowOperation("Intersect", rows);
    }

    /**
     * Creates a Difference query.
     * <p>
     * Difference returns all of the bits from the first ROW_CALL argument
     *   passed to it, without the bits from each subsequent ROW_CALL.
     *
     * @param rows 1 or more rows to differentiate
     * @return a PQL query
     * @throws IllegalArgumentException if the number of rows is less than 1
     * @see <a href="https://www.pilosa.com/docs/query-language/#difference">Difference Query</a>
     */
    @SuppressWarnings("WeakerAccess")
    public PqlRowQuery difference(PqlRowQuery... rows) {
        if (rows.length < 1) {
            throw new IllegalArgumentException("Difference operation requires at least 1 row");
        }
        return rowOperation("Difference", rows);
    }

    /**
     * Creates an Xor query.
     *
     * @param rows 2 or more rows to xor
     * @return a PQL query
     * @throws IllegalArgumentException if the number of rows is less than 2
     * @see <a href="https://www.pilosa.com/docs/query-language/#xor">Xor Query</a>
     */
    public PqlRowQuery xor(PqlRowQuery... rows) {
        if (rows.length < 2) {
            throw new IllegalArgumentException("Difference operation requires at least 2 rows");
        }
        return rowOperation("Xor", rows);
    }

    public PqlRowQuery not(PqlRowQuery row) {
        return pqlRowQuery(String.format("Not(%s)", row.serialize().getQuery()));
    }

    /**
     * Creates a Count query.
     * <p>
     * Count returns the number of set bits in the ROW_CALL passed in.
     *
     * @param rows the row query
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#count">Count Query</a>
     */
    public PqlBaseQuery count(PqlRowQuery rows) {
        return pqlQuery(String.format("Count(%s)", rows.serialize().getQuery()), false);
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
        String attributesString = Util.createAttributesString(mapper, attributes);
        boolean hasKeys = this.getOptions().isKeys();
        return pqlQuery(String.format("SetColumnAttrs(%d,%s)", id, attributesString), hasKeys);
    }

    /**
     * Creates a SetColumnAttrs query. (Enterprise version)
     * <p>
     * SetColumnAttrs associates arbitrary key/value pairs with a column in an index.
     * <p>
     * Following object types are accepted:
     * <ul>
     * <li>Long</li>
     * <li>String</li>
     * <li>Boolean</li>
     * <li>Double</li>
     * </ul>
     *
     * @param key        column key
     * @param attributes column attributes
     * @return a PQL query
     * @see <a href="https://www.pilosa.com/docs/query-language/#setcolumnattrs">SetColumnAttrs Query</a>
     */
    public PqlBaseQuery setColumnAttrs(String key, Map<String, Object> attributes) {
        String attributesString = Util.createAttributesString(mapper, attributes);
        String text = String.format("SetColumnAttrs('%s',%s)", key, attributesString);
        return pqlQuery(text, this.getOptions().isKeys());
    }

    public Map<String, Field> getFields() {
        return this.fields;
    }

    public IndexOptions getOptions() {
        return this.options;
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
                rhs.fields.equals(this.fields) &&
                rhs.getOptions().equals(this.options);
    }

    @Override
    public int hashCode() {
        // note that we don't include fields in the hash
        return new HashCodeBuilder(31, 47)
                .append(this.name)
                .toHashCode();
    }

    PqlBaseQuery pqlQuery(String query, boolean hasKeys) {
        PqlBaseQuery q = new PqlBaseQuery(query, this);
        if (hasKeys) {
            q.query.setWriteKeys(true);
        }
        return q;
    }

    PqlRowQuery pqlRowQuery(String query) {
        return new PqlRowQuery(query, this);
    }

    Index(Index index) {
        this(index.name, index.options);
        for (Map.Entry<String, Field> entry : index.fields.entrySet()) {
            // we don't copy field options, since FieldOptions has no mutating methods
            this.field(entry.getKey(), entry.getValue().getOptions());
        }
    }

    private PqlRowQuery rowOperation(String name, PqlRowQuery... rows) {
        if (rows.length == 0) {
            return pqlRowQuery(String.format("%s()", name));
        }
        StringBuilder builder = new StringBuilder(rows.length - 1);
        SerializedQuery q = rows[0].serialize();
        builder.append(q.getQuery());
        for (int i = 1; i < rows.length; i++) {
            builder.append(",");
            q = rows[i].serialize();
            builder.append(q.getQuery());
        }
        String text = String.format("%s(%s)", name, builder.toString());
        return pqlRowQuery(text);
    }

    private Index(String name, IndexOptions options) {
        this.name = name;
        this.options = options;
    }

    private String name;
    private IndexOptions options;
    private static ObjectMapper mapper = new ObjectMapper();
    private Map<String, Field> fields = new HashMap<>();
}
