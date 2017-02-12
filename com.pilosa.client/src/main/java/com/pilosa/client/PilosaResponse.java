package com.pilosa.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class PilosaResponse {
    private List<Object> results;
    private String errorMessage;
    private boolean isError = false;

    public PilosaResponse(InputStream src) throws IOException {
        parse(src);
    }

    public static PilosaResponse error(String errorMessage) {
        PilosaResponse response = new PilosaResponse();
        response.isError = true;
        response.errorMessage = errorMessage;
        return response;
    }

    protected PilosaResponse() {
        this.results = new ArrayList<>(0);
        this.isError = false;
    }

    public List<Object> getResults() {
        return results;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isError() {
        return isError;
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
    }
}
