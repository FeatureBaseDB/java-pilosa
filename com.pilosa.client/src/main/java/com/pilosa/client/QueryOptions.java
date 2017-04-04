package com.pilosa.client;

public class QueryOptions {

    public static QueryOptions defaultOptions() {
        return new QueryOptions();
    }

    public boolean isProfiles() {
        return profiles;
    }

    public void setProfiles(boolean profiles) {
        this.profiles = profiles;
    }

    private QueryOptions() {
    }

    private boolean profiles = false;
}
