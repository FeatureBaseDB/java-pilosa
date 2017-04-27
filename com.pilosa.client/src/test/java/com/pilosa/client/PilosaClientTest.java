package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.status.FrameInfo;
import com.pilosa.client.status.IndexInfo;
import com.pilosa.client.status.NodeInfo;
import com.pilosa.client.status.StatusInfo;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class PilosaClientTest {
    @Test
    public void defaultClientTest() throws IOException {
        try (PilosaClient client = PilosaClient.defaultClient()) {
            assertNotNull(client);
        }
    }

    // Note that following tests need access to internal methods, that's why they are here.
    @Test(expected = PilosaException.class)
    public void fetchFrameNodesTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("non-existent-domain-555.com:19000")) {
            client.fetchFrameNodes("foo", 0);
        }
    }

    @Test(expected = PilosaException.class)
    public void importNodeTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("non-existent-domain-555.com:19000")) {
            Internal.ImportRequest request = Internal.ImportRequest.newBuilder().build();
            client.importNode(request);
        }
    }

    @Test
    public void statusMessageFromInputStreamTest() throws IOException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL uri = loader.getResource("status1.json");
        if (uri == null) {
            fail("status1.json not found");
        }
        FileInputStream stream = new FileInputStream(uri.getFile());
        StatusMessage msg = StatusMessage.fromInputStream(stream);
        StatusInfo info = msg.getStatus();
        assertNotNull(info);
        assertEquals(1, info.getNodes().size());
        NodeInfo nodeInfo = info.getNodes().get(0);
        assertEquals(":10101", nodeInfo.getHost());
        assertEquals(2, nodeInfo.getIndexes().size());
        IndexInfo indexInfo = nodeInfo.getIndexes().get(0);
        assertEquals("mi", indexInfo.getName());
        assertEquals("col_id", indexInfo.getColumnLabel());
        assertEquals(1, indexInfo.getFrames().size());
        FrameInfo frameInfo = indexInfo.getFrames().get(0);
        assertEquals("mf10", frameInfo.getName());
        assertEquals("id", frameInfo.getRowLabel());
        assertEquals(true, frameInfo.isInverseEnabled());
        assertEquals(TimeQuantum.YEAR_MONTH_DAY, frameInfo.getTimeQuantum());
    }
}
