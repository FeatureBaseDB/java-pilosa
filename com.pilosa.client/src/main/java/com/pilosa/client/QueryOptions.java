package com.pilosa.client;

public class QueryOptions {
    public static class Builder {
        private Builder() {
        }

        public QueryOptions build() {
            return new QueryOptions(this.profiles);
        }

        public Builder setProfiles(boolean profiles) {
            this.profiles = profiles;
            return this;
        }

        private boolean profiles = false;
    }

    @SuppressWarnings("WeakerAccess")
    public static QueryOptions defaultOptions() {
        return new Builder().build();
    }

    @SuppressWarnings("WeakerAccess")
    public boolean isProfiles() {
        return profiles;
    }

    public static Builder builder() {
        return new Builder();
    }

    private QueryOptions(boolean profiles) {
        this.profiles = profiles;
    }

    private boolean profiles;
}
