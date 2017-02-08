package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class PilosaURITest {
    @Test
    public void testDefault() {
        PilosaURI uri = new PilosaURI();
        compare(uri, "http", "localhost", 15000);
    }

    @Test
    public void testFull() {
        PilosaURI uri = PilosaURI.parse("http+protobuf://db1.pilosa.com:3333");
        compare(uri, "http+protobuf", "db1.pilosa.com", 3333);
    }

    @Test
    public void testFullWithIPv4Host() {
        PilosaURI uri = PilosaURI.parse("http+protobuf://192.168.1.26:3333");
        compare(uri, "http+protobuf", "192.168.1.26", 3333);
    }

    @Test
    public void testHostOnly() {
        PilosaURI uri = PilosaURI.parse("db1.pilosa.com");
        compare(uri, "http", "db1.pilosa.com", 15000);
    }

    @Test
    public void testPortOnly() {
        PilosaURI uri = PilosaURI.parse(":5888");
        compare(uri, "http", "localhost", 5888);
    }

    @Test
    public void testHostPort() {
        PilosaURI uri = PilosaURI.parse("db1.big-data.com:5888");
        compare(uri, "http", "db1.big-data.com", 5888);
    }

    @Test
    public void testSchemeHost() {
        PilosaURI uri = PilosaURI.parse("https://db1.big-data.com");
        compare(uri, "https", "db1.big-data.com", 15000);
    }

    @Test
    public void testSchemePort() {
        PilosaURI uri = PilosaURI.parse("https://:5553");
        compare(uri, "https", "localhost", 5553);
    }

    // TODO: Tests for invalid addresses

    private void compare(PilosaURI uri, String scheme, String host, int port) {
        assertEquals(scheme, uri.getScheme());
        assertEquals(host, uri.getHost());
        assertEquals(port, uri.getPort());
    }
}
