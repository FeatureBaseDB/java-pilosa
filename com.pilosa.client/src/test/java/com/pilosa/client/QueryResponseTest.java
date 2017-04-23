package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(UnitTest.class)
public class QueryResponseTest {
    @Test
    public void testDefaultConstructor() {
        QueryResponse response = new QueryResponse();
        assertTrue(response.isSuccess());
        assertNull(response.getErrorMessage());
    }

    @Test
    public void testNoResult() {
        QueryResponse response = new QueryResponse();
        assertNull(response.getResult());
        assertNull(response.getColumns());
    }
}
