package com.pilosa.client;

public class QueryOptions {
    public static class Builder {
        private Builder() {
        }

        public QueryOptions build() {
            return new QueryOptions(this.columns);
        }

        public Builder setColumns(boolean columns) {
            this.columns = columns;
            return this;
        }

        private boolean columns = false;
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryOptions defaultOptions() {
        return new Builder().build();
    }

    @SuppressWarnings("WeakerAccess")
    public boolean isColumns() {
        return columns;
    }

    public static Builder builder() {
        return new Builder();
    }

    private QueryOptions(boolean columns) {
        this.columns = columns;
    }

    private boolean columns;
}
