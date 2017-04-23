package com.pilosa.client;

import com.pilosa.client.orm.Index;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class QueryRequestTest {
    @Test
    public void testSetTimeQuantum() {
        QueryRequest qr = QueryRequest.withIndex(Index.withName("foo"));
        qr.setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY_HOUR);
    }

    @Test
    public void testProtobuf() {
        QueryRequest qr = QueryRequest.withIndex(Index.withName("somedb"));
        qr.setQuery("Range(id=1, frame='foo', start='2016-01-01T13:00', end='2017-01-01T14:00')");
        qr.setTimeQuantum(TimeQuantum.YEAR_MONTH);
        qr.setRetrieveProfiles(true);
        Internal.QueryRequest request = qr.toProtobuf();
        assertEquals("Range(id=1, frame='foo', start='2016-01-01T13:00', end='2017-01-01T14:00')", request.getQuery());
        assertEquals("YM", request.getQuantum());
        assertEquals(true, request.getColumnAttrs());
    }

    @Test
    public void testGetQuery() {
        QueryRequest qr = QueryRequest.withIndex(Index.withName("mydb"));
        qr.setQuery("SetBit(id=1, frame='the-frame', col_id=556");
        assertEquals("SetBit(id=1, frame='the-frame', col_id=556", qr.getQuery());
    }
}
