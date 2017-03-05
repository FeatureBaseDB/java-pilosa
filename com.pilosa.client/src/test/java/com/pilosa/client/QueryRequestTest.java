package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class QueryRequestTest {
    @Test
    public void queryRequestWithTimeQuantumTest() {
        QueryRequest qr = QueryRequest.withDatabase("foo");
        qr.setTimeQuantum("YMDH");
        assertEquals("db=foo&time_granularity=YMDH", qr.toURLQueryString());
    }

    @Test(expected = PilosaException.class)
    public void queryRequestWithInvalidTimeQuantumTest() {
        QueryRequest.withDatabase("foo").setTimeQuantum("YMDHM");
    }

    @Test
    public void queryRequestToProtobufTest() {
        QueryRequest qr = QueryRequest.withDatabase("somedb");
        qr.setQuery("Range(id=1, frame='foo', start='2016-01-01T13:00', end='2017-01-01T14:00')");
        qr.setTimeQuantum("YM");
        qr.setRetrieveProfiles(true);
        Internal.QueryRequest request = qr.toProtobuf();
        assertEquals("somedb", request.getDB());
        assertEquals("Range(id=1, frame='foo', start='2016-01-01T13:00', end='2017-01-01T14:00')", request.getQuery());
        assertEquals("YM", request.getQuantum());
        assertEquals(true, request.getProfiles());
    }
}
