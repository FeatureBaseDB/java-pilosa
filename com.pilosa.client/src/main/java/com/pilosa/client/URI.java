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
     * Create the default URI.
     * @return default URI
     */
    @SuppressWarnings("WeakerAccess")
    public static URI defaultURI() {
        return new URI();
    }

    /**
     * Create a URI by specifying host and port but using the default scheme.
     * @param host is hostname or IP address of the Pilosa server
     * @param port is port of the Pilosa server
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
     * @param address is Pilosa server's address
     * @throws PilosaURIException if the address is malformed
     * @return a URI
     */
    public static URI address(String address) {
        URI uri = new URI();
        uri._parse(address);
        return uri;
    }

    /**
     * Returns the protocol part of the URI.
     * @return the protocol part of the URI
     */
    @SuppressWarnings("WeakerAccess")
    public String getScheme() {
        return scheme;
    }

    /**
     * Returns the host part of the URI.
     * @return the host part of the URI
     */
    @SuppressWarnings("WeakerAccess")
    public String getHost() {
        return host;
    }

    /**
     * Returns the port part of the URI.
     * @return the port part of the URI
     */
    @SuppressWarnings("WeakerAccess")
    public int getPort() {
        return port;
    }

    /**
     * Returns normalized address, ready to be used with an HttpClent.
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
    private final static Pattern uriPattern = Pattern.compile("^(([+a-z]+):\\/\\/)?([0-9a-z.-]+)?(:([0-9]+))?$");
}
