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

package integrationtest;

import com.pilosa.client.*;
import com.pilosa.client.exceptions.FrameExistsException;
import com.pilosa.client.exceptions.IndexExistsException;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.*;
import com.pilosa.client.status.FrameInfo;
import com.pilosa.client.status.IndexInfo;
import com.pilosa.client.status.NodeInfo;
import com.pilosa.client.status.StatusInfo;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;

import static org.junit.Assert.*;

// Note that this integration test creates many random indexes.
// It's recommended to run an ephemeral Pilosa server.
// E.g., with docker:
// $ docker run -it --rm --name pilosa -p 10101:10101 pilosa:latest

@Category(IntegrationTest.class)
public class PilosaClientIT {
    private Schema schema;
    private Index colIndex;
    private Index index;
    private Frame frame;
    private final static String SERVER_ADDRESS = ":10101";

    @Before
    public void setUp() throws IOException {
        this.schema = Schema.defaultSchema();
        this.index = schema.index(getRandomIndexName());
        try (PilosaClient client = getClient()) {
            client.createIndex(this.index);
            client.createFrame(this.index.frame("another-frame"));
            client.createFrame(this.index.frame("test"));
            client.createFrame(this.index.frame("count-test"));
            client.createFrame(this.index.frame("topn_test"));

            IndexOptions indexOptions = IndexOptions.builder()
                    .setColumnLabel("user")
                    .build();
            this.colIndex = schema.index(this.index.getName() + "-opts", indexOptions);
            client.createIndex(this.colIndex);

            FrameOptions frameOptions = FrameOptions.builder()
                    .setRowLabel("project")
                    .build();
            this.frame = this.colIndex.frame("collab", frameOptions);
            client.createFrame(this.frame);
        }
    }

    @After
    public void tearDown() throws IOException {
        try (PilosaClient client = getClient()) {
            client.deleteIndex(this.index);
            client.deleteIndex(this.colIndex);
        }
    }

    @Test
    public void createClientTest() throws IOException {
        try (PilosaClient client = PilosaClient.withURI(URI.address(":10101"))) {
            assertNotNull(client);
        }
        try (PilosaClient client = PilosaClient.withCluster(Cluster.defaultCluster())) {
            assertNotNull(client);
        }
    }

    @Test
    public void createIndexWithTimeQuantumTest() throws IOException {
        IndexOptions options = IndexOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR)
                .build();
        Index index = Index.withName("index-with-timequantum", options);
        try (PilosaClient client = getClient()) {
            client.ensureIndex(index);
            try {
                StatusInfo status = client.readStatus();
                IndexInfo info = findIndexInfo(status, index);
                assertNotNull(info);
                assertEquals(TimeQuantum.YEAR, info.getOptions().getTimeQuantum());
            } finally {
                client.deleteIndex(index);
            }
        }
    }

    @Test
    public void createFrameWithTimeQuantumTest() throws IOException {
        FrameOptions options = FrameOptions.builder()
                .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
                .build();
        Frame frame = this.index.frame("frame-with-timequantum", options);
        try (PilosaClient client = getClient()) {
            client.ensureFrame(frame);
            StatusInfo status = client.readStatus();
            FrameInfo info = findFrameInfo(status, frame);
            assertNotNull(info);
            assertEquals(TimeQuantum.YEAR_MONTH_DAY, info.getOptions().getTimeQuantum());
        }
    }

    @Test
    public void queryTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Frame frame = this.index.frame("query-test");
            client.ensureFrame(frame);
            QueryResponse response = client.query(frame.setBit(555, 10));
            assertNotNull(response.getResult());
        }
    }

    @Test
    public void queryWithColumnsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Frame frame = this.index.frame("query-test");
            client.ensureFrame(frame);
            client.query(frame.setBit(100, 1000));
            Map<String, Object> columnAttrs = new HashMap<>(1);
            columnAttrs.put("name", "bombo");
            client.query(this.index.setColumnAttrs(1000, columnAttrs));
            QueryOptions queryOptions = QueryOptions.builder()
                    .setColumns(true)
                    .build();
            QueryResponse response = client.query(frame.bitmap(100), queryOptions);
            assertNotNull(response.getColumn());
            assertEquals(1000, response.getColumn().getID());
            assertEquals(columnAttrs, response.getColumn().getAttributes());

            response = client.query(frame.bitmap(300));
            assertNull(response.getColumn());
        }
    }

    @Test
    public void protobufCreateIndexDeleteIndexTest() throws IOException {
        final Index dbname = Index.withName("to-be-deleted-" + this.index.getName());
        Frame frame = dbname.frame("delframe");
        try (PilosaClient client = getClient()) {
            try {
                client.createIndex(dbname);
                client.createFrame(frame);
                client.query(frame.setBit(1, 2));
            } finally {
                client.deleteIndex(dbname);
            }
        }
    }

    @Test
    public void createIndexWithColumnLabelFrameWithRowLabel() throws IOException {
        IndexOptions dbOptions = IndexOptions.builder()
                .setColumnLabel("cols")
                .build();
        final Index db = Index.withName("db-col-label-" + this.index.getName(), dbOptions);
        FrameOptions frameOptions = FrameOptions.builder()
                .setRowLabel("rowz")
                .build();
        try (PilosaClient client = getClient()) {
            client.createIndex(db);
            client.createFrame(db.frame("my-frame", frameOptions));
            client.deleteIndex(db);
        }
    }

    @Test(expected = PilosaException.class)
    public void failedConnectionTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("http://non-existent-sub.pilosa.com:22222")) {
            client.query(this.frame.setBit(15, 10));
        }
    }

    @Test(expected = PilosaException.class)
    public void unknownSchemeTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("notknown://:15555")) {
            client.query(this.frame.setBit(15, 10));
        }
    }

    @Test(expected = PilosaException.class)
    public void parseErrorTest() throws IOException {
        try (PilosaClient client = getClient()) {
            client.query(this.index.rawQuery("SetBit(id=5, frame=\"test\", col_id:=10)"));
        }
    }

    @Test
    public void ormCountTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Frame countFrame = this.index.frame("count-test");
            client.ensureFrame(countFrame);
            PqlBatchQuery qry = this.index.batchQuery();
            qry.add(countFrame.setBit(10, 20));
            qry.add(countFrame.setBit(10, 21));
            qry.add(countFrame.setBit(15, 25));
            client.query(qry);
            QueryResponse response = client.query(this.index.count(countFrame.bitmap(10)));
            assertEquals(2, response.getResult().getCount());
        }
    }

    @Test
    public void newOrmTest() throws IOException {
        try (PilosaClient client = getClient()) {
            client.query(this.frame.setBit(10, 20));
            QueryResponse response1 = client.query(this.frame.bitmap(10));
            assertEquals(0, response1.getColumns().size());
            BitmapResult bitmap1 = response1.getResult().getBitmap();
            assertEquals(0, bitmap1.getAttributes().size());
            assertEquals(1, bitmap1.getBits().size());
            assertEquals(20, (long) bitmap1.getBits().get(0));

            Map<String, Object> columnAttrs = new HashMap<>(1);
            columnAttrs.put("name", "bombo");
            client.query(this.colIndex.setColumnAttrs(20, columnAttrs));
            QueryOptions queryOptions = QueryOptions.builder()
                    .setColumns(true)
                    .build();
            QueryResponse response2 = client.query(this.frame.bitmap(10), queryOptions);
            ColumnItem column = response2.getColumn();
            assertNotNull(column);
            assertEquals(20, column.getID());

            Map<String, Object> bitmapAttrs = new HashMap<>(1);
            bitmapAttrs.put("active", true);
            bitmapAttrs.put("unsigned", 5);
            bitmapAttrs.put("height", 1.81);
            bitmapAttrs.put("name", "Mr. Pi");
            client.query(this.frame.setRowAttrs(10, bitmapAttrs));
            QueryResponse response3 = client.query(this.frame.bitmap(10));
            BitmapResult bitmap = response3.getResult().getBitmap();
            assertEquals(1, bitmap.getBits().size());
            assertEquals(4, bitmap.getAttributes().size());
            assertEquals(true, bitmap.getAttributes().get("active"));
            assertEquals(5L, bitmap.getAttributes().get("unsigned"));
            assertEquals(1.81, bitmap.getAttributes().get("height"));
            assertEquals("Mr. Pi", bitmap.getAttributes().get("name"));

            Frame topnFrame = this.index.frame("topn_test");
            client.query(topnFrame.setBit(155, 551));
            QueryResponse response4 = client.query(topnFrame.topN(1));
            List<CountResultItem> items = response4.getResult().getCountItems();
            assertEquals(1, items.size());
            CountResultItem item = items.get(0);
            assertEquals(155, item.getID());
            assertEquals(1, item.getCount());
        }
    }

    @Test
    public void queryInverseBitmapTest() throws IOException {
        try (PilosaClient client = getClient()) {
            FrameOptions options = FrameOptions.builder()
                    .setRowLabel("row_label")
                    .setInverseEnabled(true)
                    .build();
            Frame f1 = this.colIndex.frame("f1-inversable", options);
            client.ensureFrame(f1);
            client.query(
                    this.colIndex.batchQuery(
                            f1.setBit(1000, 5000),
                            f1.setBit(1000, 6000),
                            f1.setBit(3000, 5000)));
            QueryResponse response = client.query(
                    this.colIndex.batchQuery(
                            f1.bitmap(1000),
                            f1.inverseBitmap(5000)));
            assertEquals(2, response.getResults().size());
            List<Long> bits1 = response.getResults().get(0).getBitmap().getBits();
            List<Long> bits2 = response.getResults().get(1).getBitmap().getBits();
            assertEquals("[5000, 6000]", bits1.toString());
            assertEquals("[1000, 3000]", bits2.toString());
        }
    }

    @Test
    public void testTopN() throws IOException, InterruptedException {
        try (PilosaClient client = getClient()) {
            client.ensureFrame(this.frame);
            Frame frame = this.index.frame("topn_test");
            client.query(this.index.batchQuery(
                    frame.setBit(10, 5),
                    frame.setBit(10, 10),
                    frame.setBit(10, 15),
                    frame.setBit(20, 5),
                    frame.setBit(30, 5)
            ));
            // XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
            Thread.sleep(10000);
            QueryResponse response = client.query(frame.topN(2));
            List<CountResultItem> items = response.getResult().getCountItems();
            assertEquals(2, items.size());
            CountResultItem item = items.get(0);
            assertEquals(10, item.getID());
            assertEquals(3, item.getCount());
        }
    }

    @Test(expected = PilosaException.class)
    public void queryFailsWithError() throws IOException {
        try (PilosaClient client = getClient()) {
            client.query(this.index.rawQuery("invalid query"));
        }
    }

    @Test(expected = IndexExistsException.class)
    public void createExistingDatabaseFails() throws IOException {
        try (PilosaClient client = getClient()) {
            client.createIndex(this.colIndex);
        }

    }

    @Test(expected = FrameExistsException.class)
    public void createExistingFrameFails() throws IOException {
        try (PilosaClient client = getClient()) {
            client.createFrame(this.frame);
        }
    }

    @Test(expected = PilosaException.class)
    public void failedDeleteIndexTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("http://non-existent-sub.pilosa.com:22222")) {
            client.deleteIndex(Index.withName("non-existent"));
        }
    }

    @Test
    public void ensureIndexExistsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Index index = Index.withName(this.index.getName() + "-ensure");
            client.ensureIndex(index);
            client.createFrame(index.frame("frm"));
            client.ensureIndex(index);  // shouldn't throw an exception
            client.deleteIndex(index);
        }
    }

    @Test
    public void ensureFrameExistsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Index index = Index.withName(this.index.getName() + "-ensure-frame");
            try {
                client.createIndex(index);
                final Frame frame = index.frame("frame");
                client.ensureFrame(frame);
                client.ensureFrame(frame); // shouldn't throw an exception
                client.query(frame.setBit(1, 10));
            } finally {
                client.deleteIndex(index);
            }
        }
    }

    @Test
    public void deleteFrameTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Frame frame = index.frame("to-delete");
            client.ensureFrame(frame);
            client.deleteFrame(frame);
            // the following should succeed
            client.createFrame(frame);
        }
    }

    @Test
    public void importTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            StaticBitIterator iterator = new StaticBitIterator();
            Frame frame = this.index.frame("importframe");
            client.ensureFrame(frame);
            client.importFrame(frame, iterator);
            PqlBatchQuery bq = index.batchQuery(
                    frame.bitmap(2),
                    frame.bitmap(7),
                    frame.bitmap(10)
            );
            QueryResponse response = client.query(bq);

            List<Long> target = Arrays.asList(3L, 1L, 5L);
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                BitmapResult br = results.get(i).getBitmap();
                assertEquals(target.get(i), br.getBits().get(0));
            }
        }
    }

    @Test
    public void getSchemaTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            Schema schema = client.readSchema();
            assertTrue(schema.getIndexes().size() > 0);
        }
    }

    @Test
    public void getEmptySchemaTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            client.deleteIndex(this.index);
            client.deleteIndex(this.colIndex);
            Schema schema = client.readSchema();
            assertTrue(schema.getIndexes().size() == 0);
        }
    }

    @Test
    public void syncSchemaTest() throws IOException {
        Index remoteIndex = Index.withName("remote-index-1");
        Frame remoteFrame = remoteIndex.frame("remote-frame-1");
        Schema schema1 = Schema.defaultSchema();
        Index index11 = schema1.index("diff-index1");
        index11.frame("frame1-1");
        index11.frame("frame1-2");
        Index index12 = schema1.index("diff-index2");
        index12.frame("frame2-1");
        schema1.index(remoteIndex.getName());

        try (PilosaClient client = this.getClient()) {
            client.ensureIndex(remoteIndex);
            client.ensureFrame(remoteFrame);
            client.syncSchema(schema1);
        } finally {
            try (PilosaClient client = this.getClient()) {
                client.deleteIndex(remoteIndex);
                client.deleteIndex(index11);
                client.deleteIndex(index12);
            }
        }
    }

    @Test
    public void rangeFrameTest() throws IOException {
        try (PilosaClient client = getClient()) {
            FrameOptions options = FrameOptions.builder()
                    .addIntField("foo", 10, 20)
                    .build();
            Frame frame = this.index.frame("rangeframe", options);
            client.ensureFrame(frame);
            client.query(this.index.batchQuery(
                    frame.setBit(1, 10),
                    frame.setBit(1, 100),
                    frame.setFieldValue(10, "foo", 11),
                    frame.setFieldValue(100, "foo", 15)
            ));
            QueryResponse response = client.query(frame.sumReduce(frame.bitmap(1), "foo"));
            assertEquals(26, response.getResult().getSum());
            assertEquals(2, response.getResult().getCount());
        }

    }

    @Test
    public void excludeAttrsBitsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Map<String, Object> attrs = new HashMap<>(1);
            attrs.put("foo", "bar");
            client.query(colIndex.batchQuery(
                    frame.setBit(1, 100),
                    frame.setRowAttrs(1, attrs)
            ));

            QueryResponse response;
            QueryOptions options;

            // test exclude bits.
            options = QueryOptions.builder()
                    .setExcludeBits(true)
                    .build();
            response = client.query(frame.bitmap(1), options);
            assertEquals(0, response.getResult().getBitmap().getBits().size());
            assertEquals(1, response.getResult().getBitmap().getAttributes().size());

            // test exclude attributes.
            options = QueryOptions.builder()
                    .setExcludeAttributes(true)
                    .build();
            response = client.query(frame.bitmap(1), options);
            assertEquals(1, response.getResult().getBitmap().getBits().size());
            assertEquals(0, response.getResult().getBitmap().getAttributes().size());

        }
    }

    @Test(expected = PilosaException.class)
    public void importFailNot200() throws IOException {
        HttpServer server = runImportFailsHttpServer();
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            StaticBitIterator iterator = new StaticBitIterator();
            try {
                client.importFrame(this.index.frame("importframe"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void importFail200Test() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/fragment/nodes");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            StaticBitIterator iterator = new StaticBitIterator();
            try {
                client.importFrame(this.index.frame("importframe"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void queryFail404Test() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/404");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.frame.setBit(15, 10));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void fail304EmptyResponseTest() throws IOException {
        HttpServer server = runContent0HttpServer("/index/foo", 304);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.createIndex(Index.withName("foo"));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failQueryEmptyResponseTest() throws IOException {
        String path = String.format("/index/%s/query", this.frame.getIndex().getName());
        HttpServer server = runContent0HttpServer(path, 304);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.frame.setBit(15, 10));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failFetchFrameNodesEmptyResponseTest() throws IOException {
        HttpServer server = runContent0HttpServer("/fragment/nodes", 204);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            StaticBitIterator iterator = new StaticBitIterator();
            try {
                client.importFrame(this.index.frame("importframe"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failStatusEmptyResponseTest() throws IOException {
        HttpServer server = runContent0HttpServer("/status", 204);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.readStatus();
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failStatus200Test() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/status");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.readStatus();
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failStatus400IOError() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer400("/status");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.readStatus();
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failOverTest() {
        Cluster c = Cluster.defaultCluster();
        for (int i = 0; i < 20; i++) {
            c.addHost(URI.address(String.format("n%d.nonexistent.net:5000", i)));
        }
        PilosaClient client = PilosaClient.withCluster(c);
        client.readStatus();
    }

    private PilosaClient getClient() {
        return PilosaClient.withAddress(SERVER_ADDRESS);
    }

    private static int counter = 0;

    private static String getRandomIndexName() {
        return String.format("testdb-%d", ++counter);
    }

    private HttpServer runImportFailsHttpServer() {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/fragment/nodes", new FragmentNodesHandler());
            server.setExecutor(null);
            server.start();
            return server;
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    private HttpServer runContentSizeLyingHttpServer(String path) {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(path, new ContentSizeLyingHandler());
            server.setExecutor(null);
            server.start();
            return server;
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    private HttpServer runContentSizeLyingHttpServer400(String path) {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(path, new ContentSizeLyingHandler(400));
            server.setExecutor(null);
            server.start();
            return server;
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    private HttpServer runContent0HttpServer(String path, int statusCode) {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(path, new Content0Handler(statusCode));
            server.setExecutor(null);
            server.start();
            return server;
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    static class FragmentNodesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange r) throws IOException {
            String response = "[{\"host\":\"localhost:15999\"}]";
            r.sendResponseHeaders(200, response.length());
            try (OutputStream os = r.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    static class ContentSizeLyingHandler implements HttpHandler {
        ContentSizeLyingHandler() {
            this(200);
        }

        ContentSizeLyingHandler(int statusCode) {
            super();
            this.statusCode = statusCode;
        }

        @Override
        public void handle(HttpExchange r) throws IOException {
            r.sendResponseHeaders(statusCode, 42);
            OutputStream os = r.getResponseBody();
            os.close();
        }

        private int statusCode;
    }

    static class Content0Handler implements HttpHandler {
        Content0Handler(int statusCode) {
            super();
            this.statusCode = statusCode;
        }

        @Override
        public void handle(HttpExchange r) throws IOException {
            r.sendResponseHeaders(this.statusCode, -1);
            r.close();
        }

        private int statusCode;
    }

    private IndexInfo findIndexInfo(StatusInfo status, Index target) {
        if (status.getNodes().size() == 0) {
            return null;
        }
        NodeInfo node = status.getNodes().get(0);
        for (IndexInfo index : node.getIndexes()) {
            if (index.getName().equals(target.getName())) {
                return index;
            }
        }
        return null;
    }

    private FrameInfo findFrameInfo(StatusInfo status, Frame target) {
        IndexInfo index = findIndexInfo(status, target.getIndex());
        if (index != null) {
            for (FrameInfo frame : index.getFrames()) {
                if (frame.getName().equals(target.getName())) {
                    return frame;
                }
            }
        }
        return null;
    }
}

class StaticBitIterator implements BitIterator {
    private List<Bit> bits;
    private int index = 0;

    StaticBitIterator() {
        this.bits = new ArrayList<>(3);
        this.bits.add(Bit.create(10, 5));
        this.bits.add(Bit.create(2, 3));
        this.bits.add(Bit.create(7, 1));
    }

    @Override
    public boolean hasNext() {
        return this.index < this.bits.size();
    }

    @Override
    public Bit next() {
        return this.bits.get(index++);
    }

    @Override
    public void remove() {
        // We have this just to avoid compilation problems on JDK 7
    }
}
