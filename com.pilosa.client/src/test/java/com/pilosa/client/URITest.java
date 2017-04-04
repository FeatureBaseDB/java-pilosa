package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaURIException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class URITest {
    @Test
    public void testDefault() {
        URI uri = URI.defaultURI();
        compare(uri, "http", "localhost", 10101);
    }

    @Test
    public void testFull() {
        URI uri = URI.fromAddress("http+protobuf://db1.pilosa.com:3333");
        compare(uri, "http+protobuf", "db1.pilosa.com", 3333);
    }

    @Test
    public void testHostPortAlternative() {
        URI uri = URI.withHostPort("db1.pilosa.com", 3333);
        compare(uri, "http", "db1.pilosa.com", 3333);
    }

    @Test
    public void testFullWithIPv4Host() {
        URI uri = URI.fromAddress("http+protobuf://192.168.1.26:3333");
        compare(uri, "http+protobuf", "192.168.1.26", 3333);
    }

    @Test
    public void testHostOnly() {
        URI uri = URI.fromAddress("db1.pilosa.com");
        compare(uri, "http", "db1.pilosa.com", 10101);
    }

    @Test
    public void testPortOnly() {
        URI uri = URI.fromAddress(":5888");
        compare(uri, "http", "localhost", 5888);
    }

    @Test
    public void testHostPort() {
        URI uri = URI.fromAddress("db1.big-data.com:5888");
        compare(uri, "http", "db1.big-data.com", 5888);
    }

    @Test
    public void testSchemeHost() {
        URI uri = URI.fromAddress("https://db1.big-data.com");
        compare(uri, "https", "db1.big-data.com", 10101);
    }

    @Test
    public void testSchemePort() {
        URI uri = URI.fromAddress("https://:5553");
        compare(uri, "https", "localhost", 5553);
    }

    @Test
    public void testNormalizedAddress() {
        URI uri2 = URI.fromAddress("https+pb://big-data.pilosa.com:6888");
        assertEquals("https://big-data.pilosa.com:6888", uri2.getNormalized());
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress1() {
        URI.fromAddress("foo:bar");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress2() {
        URI.fromAddress("http://foo:");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress3() {
        URI.fromAddress("foo:");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress4() {
        URI.fromAddress(":bar");
    }

    @Test
    public void testToString() {
        URI uri = URI.defaultURI();
        assertEquals("http://localhost:10101", "" + uri);
    }

    @Test
    public void testEquals() {
        URI uri1 = URI.withHostPort("pilosa.com", 1337);
        URI uri2 = URI.fromAddress("http://pilosa.com:1337");
        boolean e = uri1.equals(uri2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (URI.defaultURI()).equals("http://localhost:10101");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        URI uri = URI.fromAddress("https://pilosa.com:1337");
        assertEquals(uri, uri);
    }

    @Test
    public void testHashCode() {
        URI uri1 = URI.defaultURI();
        URI uri2 = URI.fromAddress("http://localhost:10101");
        assertEquals(uri1.hashCode(), uri2.hashCode());
    }

    private void compare(URI uri, String scheme, String host, int port) {
        assertEquals(scheme, uri.getScheme());
        assertEquals(host, uri.getHost());
        assertEquals(port, uri.getPort());
    }
}
