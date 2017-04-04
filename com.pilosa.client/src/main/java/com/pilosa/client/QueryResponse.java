package com.pilosa.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the response from a Pilosa query.
 */
public final class QueryResponse {
    private List<QueryResult> results;
    private List<ProfileItem> profiles;
    private String errorMessage;
    private boolean isError = false;

    /**
     * Creates a default response.
     * <p>
     * This constructor is not available outside of this package.
     */
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
     * @return first result in the response
     */
    public QueryResult getResult() {
        if (this.results == null || this.results.size() == 0) {
            return null;
        }
        return this.results.get(0);
    }

    /**
     * Returns the list of profiles.
     * <p>
     * The response contains the profiles if <code>PilosaClient.query()</code> is used instead of <code>PilosaClient.query()</code>.
     *
     * @return list of profiles or <code>null</code> if the response did not have its profiles field set.
     */
    public List<ProfileItem> getProfiles() {
        return this.profiles;
    }

    /**
     * Returns the first profile in the response.
     * <p>
     * The response contains the profiles if <code>PilosaClient.query()</code> is used instead of <code>PilosaClient.query()</code>.
     *
     * @return the first profile or <code>null</code> if the response did not have its profiles field set.
     */
    public ProfileItem getProfile() {
        if (this.profiles == null || this.profiles.size() == 0) {
            return null;
        }
        return this.profiles.get(0);
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

        ArrayList<ProfileItem> profiles = new ArrayList<>(response.getProfilesCount());
        for (Internal.Profile profile : response.getProfilesList()) {
            profiles.add(ProfileItem.fromInternal(profile));
        }
        this.profiles = profiles;
    }

}
