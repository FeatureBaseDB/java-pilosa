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
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Pilosa URI.
 * <p>
 * A Pilosa URI consists of three parts:
 * <ul>
 * <li><b>Scheme</b>: Protocol of the URI. Default: <code>http</code>.</li>
 * <li><b>Host</b>: Hostname or IP URI. Default: <code>localhost</code>.</li>
 * <li><b>Port</b>: Port of the URI. Default <code>10101</code>.</li>
 * </ul>
 * <p>
 * All parts of the URI are optional. The following are equivalent:
 * <ul>
 * <li><code>http://localhost:10101</code></li>
 * <li><code>http://localhost</code></li>
 * <li><code>http://:10101</code></li>
 * <li><code>localhost:10101</code></li>
 * <li><code>localhost</code></li>
 * <li><code>:10101</code></li>
 * </ul>
 */
public final class URI {
    /**
     * Creates the default URI.
     *
     * @return default URI
     */
    @SuppressWarnings("WeakerAccess")
    public static URI defaultURI() {
        return new URI();
    }

    /**
     * Creates a URI by specifying host and port but using the default scheme.
     *
     * @param host is the hostname or IP address of the Pilosa server
     * @param port is the port of the Pilosa server
     * @return a URI
     */
    @SuppressWarnings("WeakerAccess")
    public static URI fromHostPort(String host, int port) {
        URI uri = new URI();
        uri.setHost(host);
        uri.setPort(port);
        return uri;
    }

    /**
     * Creates a URI from an address.
     *
     * @param address is Pilosa server's address
     * @return a URI
     * @throws PilosaURIException if the address is malformed
     */
    public static URI address(String address) {
        URI uri = new URI();
        uri._parse(address);
        return uri;
    }

    @SuppressWarnings("WeakerAccess")
    public String getScheme() {
        return scheme;
    }

    @SuppressWarnings("WeakerAccess")
    public String getHost() {
        return host;
    }

    @SuppressWarnings("WeakerAccess")
    public int getPort() {
        return port;
    }

    /**
     * Returns normalized address, ready to be used with an HttpClient.
     *
     * @return normalized address by keeping the scheme part up to + (plus) character
     */
    String getNormalized() {
        String scheme = this.scheme;
        int plusIndex = scheme.indexOf('+');
        if (plusIndex > 0) {
            scheme = scheme.substring(0, plusIndex);
        }
        return String.format("%s://%s:%s", scheme, this.host, this.port);
    }

    @Override
    public String toString() {
        return String.format("%s://%s:%s", this.scheme, this.host, this.port);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof URI)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        URI rhs = (URI) obj;
        return (rhs.scheme.equals(this.scheme) && rhs.host.equals(this.host) && rhs.port == this.port);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47)
                .append(this.scheme)
                .append(this.host)
                .append(this.port)
                .toHashCode();
    }

    private void setScheme(String scheme) {
        this.scheme = scheme;
    }

    private void setHost(String host) {
        this.host = host;
    }

    private void setPort(int port) {
        this.port = port;
    }

    private void _parse(String s) {
        Matcher m = uriPattern.matcher(s);
        if (m.find()) {
            String scheme = m.group(2);
            if (scheme != null) {
                setScheme(scheme);
            }
            String host = m.group(3);
            if (host != null) {
                setHost(host);
            }
            String sPort = m.group(5);
            if (sPort != null) {
                setPort(Integer.valueOf(sPort));
            }
        } else {
            throw new PilosaURIException("Not a Pilosa URI");
        }
    }

    private URI() {
    }

    private String scheme = "http";
    private String host = "localhost";
    private int port = 10101;
    private boolean isIPv6 = false;
    private final static Pattern uriPattern = Pattern.compile("^(([+a-z]+):\\/\\/)?([0-9a-z.-]+|\\[[:0-9a-fA-F]+\\])?(:([0-9]+))?$");
}
