package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class ClientIT {
    @Test
    public void queryTest() {
        Client client = new Client("http://localhost:15000");
        PilosaResponse response = client.query("testDB", "SetBit(id=5, frame=\"test\", profileID=10)");
        assertTrue(response.isSuccess());
    }

    @Test(expected = PilosaException.class)
    public void failedConnectionTest() {
        Client client = new Client("http://non-existent-sub.pilosa.com:22222");
        client.query("test2DB", "SetBit(id=15, frame=\"test\", profileID=10)");
    }

    @Test(expected = PilosaException.class)
    public void parseErrorTest() {
        Client client = new Client("http://localhost:15000");
        client.query("testDB", "SetBit(id=5, frame=\"test\", profileID:=10)");
    }
}
