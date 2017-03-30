package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ClientOptionsTest {
    @Test
    public void testCreateDefaults() {
        ClientOptions options = new ClientOptions();
        assertEquals(10, options.getConnectionPoolSizePerRoute());
        assertEquals(100, options.getConnectionPoolTotalSize());
        assertEquals(30000, options.getConnectTimeout());
        assertEquals(300000, options.getSocketTimeout());
        assertEquals(3, options.getRetryCount());
    }

    @Test
    public void testCreate() {
        ClientOptions options = new ClientOptions();
        options.setConnectionPoolSizePerRoute(2);
        options.setConnectionPoolTotalSize(50);
        options.setConnectTimeout(100);
        options.setSocketTimeout(1000);
        options.setRetryCount(5);
        assertEquals(2, options.getConnectionPoolSizePerRoute());
        assertEquals(50, options.getConnectionPoolTotalSize());
        assertEquals(100, options.getConnectTimeout());
        assertEquals(1000, options.getSocketTimeout());
        assertEquals(5, options.getRetryCount());
    }
}
