package com.pilosa.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the response from a Pilosa query.
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

    private void parseProtobuf(InputStream src) throws IOException {
        Internal.QueryResponse response = Internal.QueryResponse.parseFrom(src);
        String errorMessage = response.getErr();
        if (!errorMessage.equals("")) {
            this.errorMessage = errorMessage;
            this.isError = true;
            return;
        }

        List<QueryResult> results = new ArrayList<>(response.getResultsCount());
        for (Internal.QueryResult result : response.getResultsList()) {
            results.add(QueryResult.fromInternal(result));
        }
        this.results = results;

        ArrayList<ColumnItem> columns = new ArrayList<>(response.getColumnAttrSetsCount());
        for (Internal.ColumnAttrSet column : response.getColumnAttrSetsList()) {
            columns.add(ColumnItem.fromInternal(column));
        }
        this.columns = columns;
    }
}
