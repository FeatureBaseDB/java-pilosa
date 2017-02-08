package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class URITest {
    @Test
    public void testDefault() {
        URI uri = new URI();
        compare(uri, "http", "localhost", 15000);
    }

    @Test
    public void testFull() {
        URI uri = new URI("http+protobuf://db1.pilosa.com:3333");
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
        compare(uri, "http", "db1.pilosa.com", 15000);
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
        compare(uri, "https", "db1.big-data.com", 15000);
    }

    @Test
    public void testSchemePort() {
        URI uri = new URI("https://:5553");
        compare(uri, "https", "localhost", 5553);
    }

    // TODO: Tests for invalid addresses

    private void compare(URI uri, String scheme, String host, int port) {
        assertEquals(scheme, uri.getScheme());
        assertEquals(host, uri.getHost());
        assertEquals(port, uri.getPort());
    }
}
