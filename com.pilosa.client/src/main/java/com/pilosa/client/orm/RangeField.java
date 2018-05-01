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

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class RangeField {
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
        String qry = String.format("Range(frame='%s', %s != null)",
                frame.getName(), this.name);
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
        String qry = String.format("Range(frame='%s', %s >< [%d,%d])",
                frame.getName(), this.name, a, b);
        return this.index.pqlBitmapQuery(qry);
    }

    /**
     * Creates a Sum query.
     * <p>
     * The frame for this query should have fields set.
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
     * The frame for this query should have fields set.
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
     * The frame for this query should have fields set.
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
     * The frame for this query should have fields set.
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
     * The frame for this query should have fields set.
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
     * The frame for this query should have fields set.
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
        String qry = String.format("SetFieldValue(frame='%s', col=%d, %s=%d)",
                frame.getName(), columnID, name, value);
        return this.index.pqlQuery(qry);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RangeField)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        RangeField rhs = (RangeField) obj;
        return rhs.name.equals(this.name);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.name)
                .toHashCode();
    }


    RangeField(Frame frame, String name) {
        this.index = frame.getIndex();
        this.frame = frame;
        this.name = name;
    }

    private PqlBitmapQuery binaryOperation(String op, long n) {
        String qry = String.format("Range(frame='%s', %s %s %d)",
                frame.getName(), this.name, op, n);
        return this.index.pqlBitmapQuery(qry);
    }

    public PqlBaseQuery valueQuery(String op, PqlBitmapQuery bitmap) {
        String qry;
        if (bitmap != null) {
            qry = String.format("%s(%s, frame='%s', field='%s')",
                    op, bitmap.serialize(), frame.getName(), name);
        } else {
            qry = String.format("%s(frame='%s', field='%s')", op, frame.getName(), name);
        }
        return this.index.pqlQuery(qry);
    }

    private Index index;
    private Frame frame;
    private String name;
}
