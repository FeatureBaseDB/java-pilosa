/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaURIException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

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
        URI uri = URI.address("http+protobuf://db1.pilosa.com:3333");
        compare(uri, "http+protobuf", "db1.pilosa.com", 3333);
    }

    @Test
    public void testHostPortAlternative() {
        URI uri = URI.fromHostPort("db1.pilosa.com", 3333);
        compare(uri, "http", "db1.pilosa.com", 3333);
    }

    @Test
    public void testFullWithIPv4Host() {
        URI uri = URI.address("http+protobuf://192.168.1.26:3333");
        compare(uri, "http+protobuf", "192.168.1.26", 3333);
    }

    @Test
    public void testFullWithIPv6Host() {
        List<Object[]> addresses = Arrays.asList(
                new Object[]{"[::1]", "http", "[::1]", 10101},
                new Object[]{"[::1]:3333", "http", "[::1]", 3333},
                new Object[]{"[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "http", "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333},
                new Object[]{"https://[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "https", "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333}
        );
        for (Object[] tuple : addresses) {
            URI uri = URI.address((String) tuple[0]);
            compare(uri, (String) tuple[1], (String) tuple[2], (Integer) tuple[3]);
        }
    }

    @Test
    public void testHostOnly() {
        URI uri = URI.address("db1.pilosa.com");
        compare(uri, "http", "db1.pilosa.com", 10101);
    }

    @Test
    public void testPortOnly() {
        URI uri = URI.address(":5888");
        compare(uri, "http", "localhost", 5888);
    }

    @Test
    public void testHostPort() {
        URI uri = URI.address("db1.big-data.com:5888");
        compare(uri, "http", "db1.big-data.com", 5888);
    }

    @Test
    public void testSchemeHost() {
        URI uri = URI.address("https://db1.big-data.com");
        compare(uri, "https", "db1.big-data.com", 10101);
    }

    @Test
    public void testSchemePort() {
        URI uri = URI.address("https://:5553");
        compare(uri, "https", "localhost", 5553);
    }

    @Test
    public void testNormalizedAddress() {
        URI uri2 = URI.address("https+pb://big-data.pilosa.com:6888");
        assertEquals("https://big-data.pilosa.com:6888", uri2.getNormalized());
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress1() {
        URI.address("foo:bar");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress2() {
        URI.address("http://foo:");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress3() {
        URI.address("foo:");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress4() {
        URI.address(":bar");
    }

    @Test(expected = PilosaURIException.class)
    public void testInvalidAddress5() {
        URI.address("fd42:4201:f86b:7e09:216:3eff:fefa:ed80");
    }

    @Test
    public void testToString() {
        URI uri = URI.defaultURI();
        assertEquals("http://localhost:10101", "" + uri);
    }

    @Test
    public void testEquals() {
        URI uri1 = URI.fromHostPort("pilosa.com", 1337);
        URI uri2 = URI.address("http://pilosa.com:1337");
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
        URI uri = URI.address("https://pilosa.com:1337");
        assertEquals(uri, uri);
    }

    @Test
    public void testHashCode() {
        URI uri1 = URI.defaultURI();
        URI uri2 = URI.address("http://localhost:10101");
        assertEquals(uri1.hashCode(), uri2.hashCode());
    }

    private void compare(URI uri, String scheme, String host, int port) {
        assertEquals(scheme, uri.getScheme());
        assertEquals(host, uri.getHost());
        assertEquals(port, uri.getPort());
    }
}
