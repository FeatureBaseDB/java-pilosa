package com.pilosa.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.exceptions.PilosaException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the response from a Pilosa query.
 */
public final class PilosaResponse {
    private List<Object> results;
    private List<ProfileItem> profiles;
    private String errorMessage;
    private boolean isError = false;

    /**
     * Creates a default response.
     * <p>
     * This constructor is not available outside of this package.
     */
    PilosaResponse() {
        this.results = new ArrayList<>(0);
        this.isError = false;
    }

    /**
     * Creates a response from the given source.
     * <p>
     * This constructor is not available outside of this package.
     *
     * @param src response from Pilosa server
     * @throws IOException if there was a problem reading from the source
     */
    PilosaResponse(InputStream src) throws IOException {
        parse(src);
    }

    /**
     * Creates an error response with the given message.
     * <p>
     * This constructor is not available outside of this package.
     *
     * @param errorMessage ditto
     * @return an error response
     */
    static PilosaResponse error(String errorMessage) {
        PilosaResponse response = new PilosaResponse();
        response.isError = true;
        response.errorMessage = errorMessage;
        return response;
    }

    /**
     * Returns the list of results.
     * <p>
     * Possible results are:
     * <ul>
     * <li><b>boolean</b>: Returned from SetBit and ClearBit calls. Indicates whether the query changed a bit or not.</li>
     * <li><b>null</b>: Returned from SetBitmapAttrs call.</li>
     * <li><b>integer</b>: Returned from Count call.</li>
     * <li><b>{@link BitmapResult}</b>: Returned from Bitmap, Union, Intersect, Difference and Range calls.</li>
     * <li><b>List of {@link CountResultItem}</b>: Returned from TopN call.</li>
     * </ul>
     *
     * @return results list
     */
    public List<Object> getResults() {
        return this.results;
    }

    /**
     * Returns the first result in the response.
     * <p>
     * Possible results are:
     * <ul>
     * <li><b>boolean</b>: Returned from SetBit and ClearBit calls. Indicates whether the query changed a bit or not.</li>
     * <li><b>null</b>: Returned from SetBitmapAttrs call.</li>
     * <li><b>integer</b>: Returned from Count call.</li>
     * <li><b>BitmapResult</b>: Returned from Bitmap, Union, Intersect, Difference and Range calls.</li>
     * <li><b>List of CountResultItem</b>: Returned from TopN call.</li>
     * </ul>
     *
     * @return first result in the response
     */
    public Object getResult() {
        if (this.results == null || this.results.size() == 0) {
            return null;
        }
        return this.results.get(0);
    }

    /**
     * Returns the list of profiles.
     * <p>
     * The response contains the profiles if <code>PilosaClient.queryWithProfiles()</code> is used instead of <code>PilosaClient.query()</code>.
     *
     * @return list of profiles or <code>null</code> if the response did not have its profiles field set.
     */
    public List<ProfileItem> getProfiles() {
        return this.profiles;
    }

    /**
     * Returns the first profile in the response.
     * <p>
     * The response contains the profiles if <code>PilosaClient.queryWithProfiles()</code> is used instead of <code>PilosaClient.query()</code>.
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
     * @return the error message or null if there is no error message
     */
    String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Returns true if the response was success.
     * @return true if the response was success, false otherwise
     */
    boolean isSuccess() {
        return !isError;
    }

    private void parse(InputStream src) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> resp;
        try {
            resp = mapper.readValue(src, new TypeReference<HashMap<String, Object>>() {
            });
        } catch (Exception ex) {
            throw new PilosaException("Invalid response, can't decode", ex);
        }

        String errorMessage = (String) resp.get("error");
        if (errorMessage != null) {
            this.errorMessage = errorMessage;
            this.isError = true;
            this.results = new ArrayList<>(0);
            this.profiles = new ArrayList<>(0);
            return;
        }
        ArrayList results = (ArrayList) resp.get("results");
        if (results == null) {
            throw new PilosaException("Invalid response, no error or results");
        }
        this.results = new ArrayList<>(results.size());
        for (Object obj : results) {
            if (obj instanceof List) {
                // this is probably a TopN result
                @SuppressWarnings("unchecked")
                List<Map<String, Integer>> listObj = (List<Map<String, Integer>>) obj;
                List<CountResultItem> countResultItems = new ArrayList<>(listObj.size());
                for (Map<String, Integer> item : listObj) {
                    Integer key = item.get("key");
                    Integer count = item.get("count");
                    if (key != null && count != null) {
                        countResultItems.add(new CountResultItem(key, count));
                    } else {
                        throw new PilosaException("Unknown result array item type");
                    }

                }
                this.results.add(countResultItems);
            } else if (obj instanceof Map) {
                // this is probably a Bitmap result
                @SuppressWarnings("unchecked")
                Map<String, Object> hashObj = (Map<String, Object>) obj;
                @SuppressWarnings("unchecked")
                Map<String, Object> attrs = (Map<String, Object>) hashObj.get("attrs");
                @SuppressWarnings("unchecked")
                List<Integer> bits = (List<Integer>) hashObj.get("bits");
                if (attrs != null && bits != null) {
                    this.results.add(new BitmapResult(attrs, bits));
                } else {
                    throw new PilosaException("Unknown result object item type");
                }
            } else if (obj == null || obj instanceof Boolean || obj instanceof Integer) {
                this.results.add(obj);
            } else {
                throw new PilosaException("Unknown result item type");
            }
        }

        ArrayList profileObjs = (ArrayList) resp.get("profiles");
        if (profileObjs != null) {
            ArrayList<ProfileItem> profiles = new ArrayList<>(profileObjs.size());
            Map<String, Object> m;
            for (Object obj : profileObjs) {
                if (obj instanceof Map) {
                    profiles.add(ProfileItem.fromMap((Map) obj));
                }
            }
            this.profiles = profiles;
        } else {
            this.profiles = new ArrayList<>(0);
        }
    }
}
