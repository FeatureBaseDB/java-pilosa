package com.pilosa.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@SuppressWarnings("WeakerAccess")
public final class Version {
    @SuppressWarnings("WeakerAccess")
    Version() {
    }

    @SuppressWarnings("WeakerAccess")
    public static String getVersion() {
        return Version.properties.getProperty("version");
    }

    @SuppressWarnings("WeakerAccess")
    public static String getBuildTime() {
        return Version.properties.getProperty("build.time");
    }

    static {
        Version version = new Version();
        InputStream resourceAsStream = version.getClass().getResourceAsStream("/version.properties");
        Version.properties = loadProperties(resourceAsStream);
    }

    static Properties loadProperties(InputStream src) {
        Properties props = new Properties();
        try {
            props.load(src);
        } catch (IOException e) {
            props.setProperty("version", "0.0.0");
            props.setProperty("build.time", "2016-11-01 09:00:00");
        }
        return props;
    }

    private static Properties properties;
}
