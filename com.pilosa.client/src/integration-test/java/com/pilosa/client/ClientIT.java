package com.pilosa.client;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class ClientIT {
    @Test
    public void queryTest() {
        PilosaClient client = new PilosaClient("http://localhost:15000");
        client.query("testDB", "ClearBit(id=5, frame=\"test\", profileID=10)");
        PilosaResponse response = client.query("testDB", "SetBit(id=5, frame=\"test\", profileID=10)");
        assertEquals(true, response.getResult());
    }

    @Test
    public void queryOrmTest() {
        PilosaClient client = new PilosaClient("http://localhost:15000");
        client.query("testDB", Pql.clearBit(5, "test", 10));
        PilosaResponse response = client.query("testDB", Pql.setBit(5, "test", 10));
        assertEquals(true, response.getResult());
    }

    @Test(expected = PilosaException.class)
    public void failedConnectionTest() {
        PilosaClient client = new PilosaClient("http://non-existent-sub.pilosa.com:22222");
        client.query("test2DB", "SetBit(id=15, frame=\"test\", profileID=10)");
    }

    @Test(expected = PilosaException.class)
    public void parseErrorTest() {
        PilosaClient client = new PilosaClient("http://localhost:15000");
        client.query("testDB", "SetBit(id=5, frame=\"test\", profileID:=10)");
    }
}
