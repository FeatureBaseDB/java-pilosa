package com.pilosa.client;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PilosaURI {
    private String scheme = "http";
    private String host = "localhost";
    private int port = 15000;
    private boolean isIPv6 = false;
    private Pattern uriPattern = Pattern.compile("(([+a-z]+):\\/\\/)?([0-9a-z.-]+)?(:([0-9]+))?");

    public PilosaURI() {
    }

    public PilosaURI(String scheme, String host, int port) {
        this.setScheme(scheme);
        this.setHost(host);
        this.setPort(port);
    }

    public PilosaURI(String address) {
        this._parse(address);
    }

    public static PilosaURI parse(String s) {
        PilosaURI uri = new PilosaURI();
        uri._parse(s);
        return uri;
    }

    public String getScheme() {
        return scheme;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return String.format("%s://%s:%s", this.scheme, this.host, this.port);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PilosaURI)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        PilosaURI rhs = (PilosaURI) obj;
        return (rhs.scheme.equals(this.scheme) && rhs.host.equals(this.host) && rhs.port == this.port);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(31, 47).
                append(this.scheme).
                append(this.host).
                append(this.port).
                toHashCode();
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
                this.scheme = scheme;
            }
            String host = m.group(3);
            if (host != null) {
                this.host = host;
            }
            String sPort = m.group(5);
            if (sPort != null) {
                this.port = Integer.valueOf(sPort);
            }
        }
    }
}
