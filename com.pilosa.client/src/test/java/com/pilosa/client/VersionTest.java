package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category(UnitTest.class)
public class VersionTest {
    @Test
    public void testVersionLoaded() {
        assertNotEquals("0.0.0", Version.getVersion());
        assertNotEquals("2016-11-01 09:00:00", Version.getBuildTime());
    }

    @Test
    public void testLoadPropertiesFailure() {
        InputStream inputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        };
        Properties props = Version.loadProperties(inputStream);
        assertEquals("0.0.0", props.getProperty("version"));
        assertEquals("2016-11-01 09:00:00", props.getProperty("build.time"));
    }
}
