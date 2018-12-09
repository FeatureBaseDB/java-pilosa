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

import com.pilosa.client.exceptions.ValidationException;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

public class Schema {
    private Schema() {
    }

    public static Schema defaultSchema() {
        return new Schema();
    }

    /**
     * Returns an index with a name.
     *
     * @param name index name
     * @return a Index object
     * @throws ValidationException if the passed index name is not valid
     */
    public Index index(String name) {
        return index(name, IndexOptions.withDefaults());
    }

    /**
     * Returns an index with a name.
     *
     * @param name    index name
     * @param options index options
     * @return a Index object
     * @throws ValidationException if the passed index name is not valid
     */
    public Index index(String name, IndexOptions options) {
        if (this.indexes.containsKey(name)) {
            return this.indexes.get(name);
        }
        Index index = Index.create(name, options);
        this.indexes.put(name, index);
        return index;
    }

    /**
     * Copies other index to this schema and returns the new index
     *
     * @param other index
     * @return copied index
     */
    public Index index(Index other) {
        Index index = this.index(other.getName(), other.getOptions());
        for (Map.Entry<String, Field> fieldEntry : other.getFields().entrySet()) {
            Field field = fieldEntry.getValue();
            index.field(field.getName(), field.getOptions());
        }
        return index;
    }

    public Schema diff(Schema other) {
        Schema result = Schema.defaultSchema();
        for (Map.Entry<String, Index> indexEntry : this.indexes.entrySet()) {
            String indexName = indexEntry.getKey();
            Index index = indexEntry.getValue();
            if (!other.indexes.containsKey(indexName)) {
                // if the index doesn't exist in the other schema, simply copy it
                result.indexes.put(indexName, new Index(indexEntry.getValue()));
            } else {
                // the index exists in the other schema; check the fields
                Index otherIndex = other.indexes.get(indexName);
                Index resultIndex = Index.create(indexName, otherIndex.getOptions());
                Map<String, Field> otherIndexFields = otherIndex.getFields();
                for (Map.Entry<String, Field> fieldEntry : index.getFields().entrySet()) {
                    String fieldName = fieldEntry.getKey();
                    if (!otherIndexFields.containsKey(fieldName)) {
                        resultIndex.field(fieldName, fieldEntry.getValue().getOptions());
                    }
                }
                // check whether we modified result index
                if (resultIndex.getFields().size() > 0) {
                    result.indexes.put(indexName, resultIndex);
                }
            }
        }
        return result;
    }

    public Map<String, Index> getIndexes() {
        return this.indexes;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Schema)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Schema rhs = (Schema) obj;
        return rhs.indexes.equals(this.indexes);
    }

    @Override
    public int hashCode() {
        // note that we don't include fields in the hash
        return new HashCodeBuilder(31, 47)
                .append(this.indexes)
                .toHashCode();
    }

    private Map<String, Index> indexes = new HashMap<>();
}
