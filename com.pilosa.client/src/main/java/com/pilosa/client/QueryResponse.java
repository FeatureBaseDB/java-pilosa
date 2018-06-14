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

package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the response from a Pilosa query.
 *
 * @see <a href="https://www.pilosa.com/docs/query-language/">Query Language</a>
 */
public final class QueryResponse {
    private List<QueryResult> results;
    private List<ColumnItem> columns;
    private String errorMessage;
    private boolean isError = false;

    QueryResponse() {
    }

    static QueryResponse fromProtobuf(InputStream src) throws IOException {
        QueryResponse response = new QueryResponse();
        response.parseProtobuf(src);
        return response;
    }

    /**
     * Returns the list of results.
     *
     * @return results list
     */
    public List<QueryResult> getResults() {
        return this.results;
    }

    /**
     * Returns the first result in the response.
     *
     * @return first result in the response or <code>null</code> if there are none
     */
    public QueryResult getResult() {
        if (this.results == null || this.results.size() == 0) {
            return null;
        }
        return this.results.get(0);
    }

    /**
     * Returns the list of columns.
     * <p>
     * The response contains the columns if {@link QueryOptions.Builder#setColumns(boolean)} was set to <code>true</code>.
     *
     * @return list of columns or <code>null</code> if the response did not have its columns field set.
     */
    public List<ColumnItem> getColumns() {
        return this.columns;
    }

    /**
     * Returns the first column in the response.
     * <p>
     * The response contains the columns if {@link QueryOptions.Builder#setColumns(boolean)} was set to <code>true</code>.
     *
     * @return the first column or <code>null</code> if the response did not have its columns field set.
     */
    public ColumnItem getColumn() {
        if (this.columns == null || this.columns.size() == 0) {
            return null;
        }
        return this.columns.get(0);
    }

    /**
     * Returns the error message in the response, if any.
     *
     * @return the error message or null if there is no error message
     */
    String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Returns true if the response was success.
     *
     * <p> This method is used internally.
     *
     * @return true if the response was success, false otherwise
     */
    boolean isSuccess() {
        return !isError;
    }

    void parseQueryResponse(Internal.QueryResponse response) {
        List<QueryResult> results = new ArrayList<>(response.getResultsCount());
        for (Internal.QueryResult q : response.getResultsList()) {
            int type = q.getType();
            switch (type) {
                case QueryResultType.ROW:
                    results.add(RowResult.fromInternal(q));
                    break;
                case QueryResultType.BOOL:
                    results.add(BoolResult.fromInternal(q));
                    break;
                case QueryResultType.INT:
                    results.add(IntResult.fromInternal(q));
                    break;
                case QueryResultType.PAIRS:
                    results.add(TopNResult.fromInternal(q));
                    break;
                case QueryResultType.VAL_COUNT:
                    results.add(ValueCountResult.fromInternal(q));
                    break;
                case QueryResultType.NIL:
                    results.add(NullResult.defaultResult());
                    break;
                default:
                    throw new PilosaException(String.format("Unknown type: %d", type));
            }
        }
        this.results = results;

        ArrayList<ColumnItem> columns = new ArrayList<>(response.getColumnAttrSetsCount());
        for (Internal.ColumnAttrSet column : response.getColumnAttrSetsList()) {
            columns.add(ColumnItem.fromInternal(column));
        }
        this.columns = columns;
    }

    private void parseProtobuf(InputStream src) throws IOException {
        Internal.QueryResponse response = Internal.QueryResponse.parseFrom(src);
        String errorMessage = response.getErr();
        if (!errorMessage.equals("")) {
            this.errorMessage = errorMessage;
            this.isError = true;
            return;
        }
        parseQueryResponse(response);
    }
}
