package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Category(UnitTest.class)
public class QueryResultTest {
    @Test
    public void testCreateDefaultConstructor() {
        new QueryResult();
    }

    @Test
    public void testEquals() {
        QueryResult q1 = new QueryResult(null, null, 1);
        QueryResult q2 = new QueryResult(null, null, 1);
        assertEquals(q1, q1);
        assertEquals(q1, q2);
        assertFalse(q1.equals(new QueryResponse()));
    }

    @Test
    public void testHashCode() {
        QueryResult q1 = new QueryResult(null, null, 1);
        QueryResult q2 = new QueryResult(null, null, 1);
        assertEquals(q1.hashCode(), q2.hashCode());
    }
}
