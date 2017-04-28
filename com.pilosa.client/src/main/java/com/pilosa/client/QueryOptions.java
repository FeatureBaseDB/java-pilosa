package com.pilosa.client;

import com.pilosa.client.orm.PqlQuery;

/**
 * Contains options to customize {@link PilosaClient#query(PqlQuery, QueryOptions)}.
 * <p>
 * In order to set options, create a {@link Builder} object using {@link QueryOptions#builder()}.
 * <p>
 * <pre>
 *  <code>
 *     QueryOptions options = QueryOptions.builder()
 *         .setColumns(true)
 *         .build();
 *  </code>
 * </pre>
 *
 * @see <a href="https://www.pilosa.com/docs/api-reference/">Pilosa API Reference</a>
 */
public class QueryOptions {
    public static class Builder {
        private Builder() {
        }

        /**
         * Enables returning column data from bitmap queries.
         *
         * @param columns set to <code>true</code> for returning column data
         * @return QueryOptions builder
         */
        public Builder setColumns(boolean columns) {
            this.columns = columns;
            return this;
        }

        /**
         * Creates the QueryOptions object.
         *
         * @return QueryOptions object
         */
        public QueryOptions build() {
            return new QueryOptions(this.columns);
        }

        private boolean columns = false;
    }

    /**
     * Creates a QueryOptions object with the defaults.
     * @return QueryOptions object
     */
    @SuppressWarnings("WeakerAccess")
    public static QueryOptions defaultOptions() {
        return new Builder().build();
    }

    @SuppressWarnings("WeakerAccess")
    public boolean isColumns() {
        return columns;
    }

    /**
     * Creates a QueryOptions.Builder object.
     * @return a Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    private QueryOptions(boolean columns) {
        this.columns = columns;
    }

    private boolean columns;
}
