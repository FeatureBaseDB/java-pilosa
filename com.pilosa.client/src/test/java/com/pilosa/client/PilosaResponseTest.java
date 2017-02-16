package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class PilosaResponseTest {
    @Test
    public void testBooleanResponse() throws IOException {
        compareResponse("{\"results\":[true]}", new Object[]{true});
        compareResponse("{\"results\":[false]}", new Object[]{false});
    }

    @Test
    public void testNullResponse() {
        compareResponse("{\"results\":[null]}", new Object[]{null});
    }

    @Test
    public void testIntegerResponse() {
        compareResponse("{\"results\":[1]}", new Object[]{1});
    }

    @Test
    public void testBitmapResponse() {
        Map<String, Object> targetAttrs = new HashMap<>(3);
        targetAttrs.put("foo", "bar");
        targetAttrs.put("zoo", 2);
        targetAttrs.put("zof", 2.37);
        targetAttrs.put("q", true);
        targetAttrs.put("nn", null);
        Map<String, Object> obj = new HashMap<>(1);
        obj.put("a", 2);
        targetAttrs.put("obj", obj);
        List<Integer> targetBits = new ArrayList<>(1);
        targetBits.add(5);
        BitmapResult target = new BitmapResult(targetAttrs, targetBits);
        compareResponse(
                "{\"results\":[{\"attrs\":{\"foo\":\"bar\", \"zoo\":2, \"zof\":2.37, \"q\":true, \"nn\":null, \"obj\":{\"a\":2}}, \"bits\":[5]}]}",
                new Object[]{target}
        );
    }

    @Test
    public void testBitmapResponseWithProfiles() {
        Map<String, Object> targetAttrs = new HashMap<>(3);
        targetAttrs.put("foo", "bar");
        targetAttrs.put("zoo", 2);
        targetAttrs.put("zof", 2.37);
        targetAttrs.put("q", true);
        targetAttrs.put("nn", null);
        Map<String, Object> obj = new HashMap<>(1);
        obj.put("a", 2);
        targetAttrs.put("obj", obj);
        List<Integer> targetBits = new ArrayList<>(1);
        targetBits.add(5);
        BitmapResult target = new BitmapResult(targetAttrs, targetBits);
        Map<String, Object> piAttrs = new HashMap<>(1);
        piAttrs.put("age", 67);
        ProfileItem pi = new ProfileItem(44, piAttrs);
        String s = "{\"results\":[{\"attrs\":{\"foo\":\"bar\", \"zoo\":2, \"zof\":2.37, \"q\":true, \"nn\":null, \"obj\":{\"a\":2}}, \"bits\":[5]}],\"profiles\":[{\"id\":44,\"attrs\":{\"age\":67}}]}";
        compareResponse(
                s,
                new Object[]{target},
                new ProfileItem[]{pi}
        );
    }

    @Test
    public void testCountResponse() {
        List<CountResultItem> target = new ArrayList<>(1);
        target.add(new CountResultItem(5, 10));
        compareResponse(
                "{\"results\":[[{\"key\":5, \"count\":10}]]}",
                new Object[]{target}
        );
    }

    @Test
    public void testProfilesResponse() {
        PilosaResponse response = createResponse("{\"results\":[{\"attrs\":{\"height\":\"zoo\"},\"bits\":[10]}],\"profiles\":[{\"id\":10,\"attrs\":{\"label\":\"zoo\"}}]}");
    }

    @Test
    public void testErrorResponse() {
        PilosaResponse response = createResponse("{\"error\":\"some error\"}");
        assertFalse(response.isSuccess());
        assertEquals("some error", response.getErrorMessage());
    }

    @Test
    public void testErrorConstructor() {
        PilosaResponse response = PilosaResponse.error("some error");
        assertFalse(response.isSuccess());
        assertEquals("some error", response.getErrorMessage());
    }

    @Test
    public void testDefaultConstructor() {
        PilosaResponse response = new PilosaResponse();
        assertTrue(response.isSuccess());
        assertNull(response.getErrorMessage());
    }

    @Test
    public void testGetResult() throws IOException {
        PilosaResponse response = new PilosaResponse();
        assertEquals(null, response.getResult());
        response = createResponse("{\"results\":[1]}");
        assertEquals(1, response.getResult());
    }

    @Test(expected = PilosaException.class)
    public void testInvalidResponse() {
        createResponse("");
    }

    @Test(expected = PilosaException.class)
    public void testInvalidResponseNoResults() {
        createResponse("{\"res\": [1]}");
    }

    @Test(expected = PilosaException.class)
    public void testInvalidResponseUnknown() {
        createResponse("{\"results\":[1.2]}");
    }

    @Test(expected = PilosaException.class)
    public void testInvalidResponseUnknownObject() {
        createResponse("{\"results\":[{\"attrs\":{}, \"Hits\":[5]}]}");
    }

    @Test(expected = PilosaException.class)
    public void testInvalidResponseUnknownArray() {
        createResponse("{\"results\":[[{\"key\":5, \"CNT\":10}]]}");
    }

    private PilosaResponse createResponse(String s) {
        InputStream src = new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
        PilosaResponse response = null;
        try {
            response = new PilosaResponse(src);
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return response;
    }

    private void compareResponse(String s, Object[] target) {
        compareResponse(s, target, null);
    }

    private void compareResponse(String s, Object[] target, ProfileItem[] profiles) {
        PilosaResponse response = createResponse(s);
        assertTrue(response.isSuccess());
        assertNull(response.getErrorMessage());
        List<Object> results = response.getResults();
        assertArrayEquals(target, results.toArray());
        if (profiles != null) {
            assertArrayEquals(profiles, response.getProfiles().toArray());
        }
    }
}
