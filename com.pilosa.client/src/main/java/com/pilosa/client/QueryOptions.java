package com.pilosa.client;

public class QueryOptions {

    public static QueryOptions defaultOptions() {
        return new QueryOptions();
    }

    public boolean isRetrieveProfiles() {
        return retrieveProfiles;
    }

    public void setRetrieveProfiles(boolean retrieveProfiles) {
        this.retrieveProfiles = retrieveProfiles;
    }

    private QueryOptions() {
    }

    private boolean retrieveProfiles = false;
}
