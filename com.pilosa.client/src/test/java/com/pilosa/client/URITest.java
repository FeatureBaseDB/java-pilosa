package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaURIException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class URITest {
    @Test
    public void testDefault() {
        URI uri = new URI();
        compare(uri, "http", "localhost", 10101);
    }

    @Test
    public void testFull() {
        URI uri = new URI("http+protobuf://db1.pilosa.com:3333");
        compare(uri, "http+protobuf", "db1.pilosa.com", 3333);
    }

    @Test
    public void testHostPortAlternative() {
        URI uri = new URI("db1.pilosa.com", 3333);
        compare(uri, "http", "db1.pilosa.com", 3333);
    }

    @Test
    public void testFullAlternative() {
        URI uri = new URI("http+protobuf", "db1.pilosa.com", 3333);
        compare(uri, "http+protobuf", "db1.pilosa.com", 3333);
    }

    @Test
    public void testFullWithIPv4Host() {
        URI uri = new URI("http+protobuf://192.168.1.26:3333");
        compare(uri, "http+protobuf", "192.168.1.26", 3333);
    }

    @Test
    public void testHostOnly() {
        URI uri = new URI("db1.pilosa.com");
        compare(uri, "http", "db1.pilosa.com", 10101);
    }

    @Test
    public void testPortOnly() {
        URI uri = new URI(":5888");
        compare(uri, "http", "localhost", 5888);
    }

    @Test
    public void testHostPort() {
        URI uri = new URI("db1.big-data.com:5888");
        compare(uri, "http", "db1.big-data.com", 5888);
    }

    @Test
    public void testSchemeHost() {
        URI uri = new URI("https://db1.big-data.com");
        compare(uri, "https", "db1.big-data.com", 10101);
    }

    @Test
    public void testSchemePort() {
        URI uri = new URI("https://:5553");
        compare(uri, "https", "localhost", 5553);
    }

    @Test
    public void testNormalizedAddress() {
        URI uri1 = new URI("http+protobuf", "big-data.pilosa.com", 6888);
        assertEquals("http://big-data.pilosa.com:6888", uri1.getNormalizedAddress());

        URI uri2 = new URI("https", "big-data.pilosa.com", 6888);
        assertEquals("https://big-data.pilosa.com:6888", uri2.getNormalizedAddress());
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress1() {
        new URI("foo:bar");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress2() {
        new URI("http://foo:");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress3() {
        new URI("foo:");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress4() {
        new URI(":bar");
    }

    @Test
    public void testToString() {
        URI uri = new URI();
        assertEquals("http://localhost:10101", "" + uri);
    }

    @Test
    public void testEquals() {
        URI uri1 = new URI("https", "pilosa.com", 1337);
        URI uri2 = new URI("https://pilosa.com:1337");
        boolean e = uri1.equals(uri2);
        assertTrue(e);
    }

    @Test
    public void testEqualsFailsWithOtherObject() {
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        boolean e = (new URI()).equals("http://localhost:10101");
        assertFalse(e);
    }

    @Test
    public void testEqualsSameObject() {
        URI uri = new URI("https", "pilosa.com", 1337);
        assertEquals(uri, uri);
    }

    @Test
    public void testHashCode() {
        URI uri1 = new URI("https", "pilosa.com", 1337);
        URI uri2 = new URI("https://pilosa.com:1337");
        assertEquals(uri1.hashCode(), uri2.hashCode());

    }

    private void compare(URI uri, String scheme, String host, int port) {
        assertEquals(scheme, uri.getScheme());
        assertEquals(host, uri.getHost());
        assertEquals(port, uri.getPort());
    }
}
