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

    @Test(expected = IllegalArgumentException.class)
    public void invalidMethodTest() {
        PilosaClient client = PilosaClient.defaultClient();
        client.makeRequest("INVALID", "/foo", null);
    }
}
