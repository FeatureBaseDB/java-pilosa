package com.pilosa.client;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Pilosa URI.
 * <p>
 * A Pilosa URI consists of three parts:
 * <ul>
 * <li><b>Scheme</b>: Protocol of the URI. Default: <code>http</code>.</li>
 * <li><b>Host</b>: Hostname or IP URI. Default: <code>localhost</code>.</li>
 * <li><b>Port</b>: Port of the URI. Default <code>15000</code>.</li>
 * </ul>
 * <p>
 * All parts of the URI are optional. The following are equivalent:
 * <ul>
 * <li><code>http://localhost:15000</code></li>
 * <li><code>http://localhost</code></li>
 * <li><code>http://:15000</code></li>
 * <li><code>localhost:15000</code></li>
 * <li><code>localhost</code></li>
 * <li><code>:15000</code></li>
 * </ul>
 */
public final class URI {
    private String scheme = "http";
    private String host = "localhost";
    private int port = 15000;
    private boolean isIPv6 = false;
    private Pattern uriPattern = Pattern.compile("^(([+a-z]+):\\/\\/)?([0-9a-z.-]+)?(:([0-9]+))?$");

    /**
     * Create the default URI. <code>http://localhost:15000</code>
     */
    public URI() {
    }

    /**
     * Create a URI by specifying host and port.
     *
     * @param host hostname or IP address
     * @param port
     */
    public URI(String host, int port) {
        this("http", host, port);
    }

    /**
     * Create a URI by specifying each part.
     *
     * @param scheme protocol of the URI
     * @param host   hostname or IP address
     * @param port
     */
    public URI(String scheme, String host, int port) {
        setScheme(scheme);
        setHost(host);
        setPort(port);
    }

    /**
     * Creates a URI by parsing from a string.
     * @param address
     * @throws URISyntaxException if the address is malformed
     */
    public URI(String address) throws URISyntaxException {
        _parse(address);
    }

    /**
     * Returns the protocol part of the URI.
     * @return the protocol part of the URI
     */
    public String getScheme() {
        return scheme;
    }

    /**
     * Returns the host part of the URI.
     * @return the host part of the URI
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the port part of the URI.
     * @return the port part of the URI
     */
    public int getPort() {
        return port;
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

    private void _parse(String s) throws URISyntaxException {
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
            throw new URISyntaxException(s, "Not a Pilosa URI");
        }
    }
}
