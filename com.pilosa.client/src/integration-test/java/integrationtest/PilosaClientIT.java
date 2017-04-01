package integrationtest;

import com.pilosa.client.*;
import com.pilosa.client.exceptions.DatabaseExistsException;
import com.pilosa.client.exceptions.FrameExistsException;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.BatchQuery;
import com.pilosa.client.orm.Database;
import com.pilosa.client.orm.Frame;
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

// Note that this integration test creates many random databases.
// It's recommended to run an ephemeral Pilosa server.
// E.g., with docker:
// $ docker run -it --rm --name pilosa -p 10101:10101 pilosa:latest

@Category(IntegrationTest.class)
public class PilosaClientIT {
    private Database database;
    private Database db;
    private Frame frame;
    private final static String SERVER_ADDRESS = ":10101";

    @Before
    public void setUp() throws IOException {
        this.db = Database.named(getRandomDatabaseName());
        try (PilosaClient client = getClient()) {
            client.createDatabase(this.db);
            client.createFrame(this.db.frame("another-frame"));
            client.createFrame(this.db.frame("test"));
            client.createFrame(this.db.frame("count-test"));
            client.createFrame(this.db.frame("topn_test"));

            DatabaseOptions dbOptions = new DatabaseOptions.Builder()
                    .setColumnLabel("user")
                    .build();
            this.database = Database.named(this.db.getName() + "-opts", dbOptions);
            client.createDatabase(this.database);

            FrameOptions frameOptions = new FrameOptions.Builder()
                    .setRowLabel("project")
                    .build();
            this.frame = this.database.frame("collab", frameOptions);
            client.createFrame(this.frame);
        }
    }

    @After
    public void tearDown() throws IOException {
        try (PilosaClient client = getClient()) {
            client.deleteDatabase(this.db);
            client.deleteDatabase(this.database);
        }
    }

    @Test
    public void createClientTest() throws IOException {
        try (PilosaClient client = new PilosaClient(new URI(":10101"))) {
            assertNotNull(client);
        }
        try (PilosaClient client = new PilosaClient(new Cluster())) {
            assertNotNull(client);
        }
    }

    @Test
    public void queryTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Frame frame = this.db.frame("query-test");
            client.ensureFrameExists(frame);
            QueryResponse response = client.query(frame.setBit(555, 10));
            assertNotNull(response.getResult());
        }
    }

    @Test
    public void queryWithProfilesTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Frame frame = this.db.frame("query-test");
            client.ensureFrameExists(frame);
            client.query(frame.setBit(100, 1000));
            Map<String, Object> profileAttrs = new HashMap<>(1);
            profileAttrs.put("name", "bombo");
            client.query(this.db.setProfileAttrs(1000, profileAttrs));
            QueryResponse response = client.queryWithProfiles(frame.bitmap(100));
            assertNotNull(response.getProfile());
            assertEquals(1000, response.getProfile().getID());
            assertEquals(profileAttrs, response.getProfile().getAttributes());

            response = client.queryWithProfiles(this.db.getName(), "Bitmap(frame='query-test', id=300)");
            assertNull(response.getProfile());
        }
    }

    @Test
    public void protobufCreateDatabaseDeleteDatabaseTest() throws IOException {
        final Database dbname = Database.named("to-be-deleted-" + this.db.getName());
        Frame frame = dbname.frame("delframe");
        try (PilosaClient client = getClient()) {
            client.createDatabase(dbname);
            client.createFrame(frame);
            client.query(frame.setBit(1, 2));
            client.deleteDatabase(dbname);
        }
    }

    @Test
    public void createDatabaseWithColumnLabelFrameWithRowLabel() throws IOException {
        DatabaseOptions dbOptions = new DatabaseOptions.Builder()
                .setColumnLabel("cols")
                .build();
        final Database db = Database.named("db-col-label-" + this.db.getName(), dbOptions);
        FrameOptions frameOptions = new FrameOptions.Builder()
                .setRowLabel("rowz")
                .build();
        try (PilosaClient client = getClient()) {
            client.createDatabase(db);
            client.createFrame(db.frame("my-frame", frameOptions));
            client.deleteDatabase(db);
        }
    }

    @Test(expected = PilosaException.class)
    public void failedConnectionTest() throws IOException {
        try (PilosaClient client = new PilosaClient("http://non-existent-sub.pilosa.com:22222")) {
            client.query("test2db", "SetBit(id=15, frame=\"test\", profileID=10)");
        }
    }

    @Test(expected = PilosaException.class)
    public void unknownSchemeTest() throws IOException {
        try (PilosaClient client = new PilosaClient("notknown://:15555")) {
            client.query("test2db", "SetBit(id=15, frame=\"test\", profileID=10)");
        }
    }

    @Test(expected = PilosaException.class)
    public void parseErrorTest() throws IOException {
        try (PilosaClient client = getClient()) {
            client.query("testdb", "SetBit(id=5, frame=\"test\", profileID:=10)");
        }
    }

    @Test
    public void ormCountTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Frame countFrame = this.db.frame("count-test");
            client.ensureFrameExists(countFrame);
            BatchQuery qry = this.db.batchQuery();
            qry.add(countFrame.setBit(10, 20));
            qry.add(countFrame.setBit(10, 21));
            qry.add(countFrame.setBit(15, 25));
            client.query(qry);
            QueryResponse response = client.query(this.db.count(countFrame.bitmap(10)));
            assertEquals(2, response.getResult().getCount());
        }
    }

    @Test
    public void newOrmTest() throws IOException {
        try (PilosaClient client = getClient()) {
            client.query(this.frame.setBit(10, 20));
            QueryResponse response1 = client.query(this.frame.bitmap(10));
            assertEquals(0, response1.getProfiles().size());
            BitmapResult result1 = response1.getResult().getBitmap();
            assertEquals(0, result1.getAttributes().size());
            assertEquals(1, result1.getBits().size());
            assertEquals(20, (long) result1.getBits().get(0));

            Map<String, Object> profileAttrs = new HashMap<>(1);
            profileAttrs.put("name", "bombo");
            client.query(this.database.setProfileAttrs(20, profileAttrs));
            QueryResponse response2 = client.queryWithProfiles(this.frame.bitmap(10));
            ProfileItem profile = response2.getProfile();
            assertNotNull(profile);
            assertEquals(20, profile.getID());

            Map<String, Object> bitmapAttrs = new HashMap<>(1);
            bitmapAttrs.put("active", true);
            bitmapAttrs.put("unsigned", 5);
            bitmapAttrs.put("height", 1.81);
            bitmapAttrs.put("name", "Mr. Pi");
            client.query(this.frame.setBitmapAttrs(10, bitmapAttrs));
            QueryResponse response3 = client.query(this.frame.bitmap(10));
            BitmapResult bitmap = response3.getResult().getBitmap();
            assertEquals(1, bitmap.getBits().size());
            assertEquals(4, bitmap.getAttributes().size());
            assertEquals(true, bitmap.getAttributes().get("active"));
            assertEquals(5L, bitmap.getAttributes().get("unsigned"));
            assertEquals(1.81, bitmap.getAttributes().get("height"));
            assertEquals("Mr. Pi", bitmap.getAttributes().get("name"));

            Frame topnFrame = this.db.frame("topn_test");
            client.query(topnFrame.setBit(155, 551));
            QueryResponse response4 = client.query(topnFrame.topN(1));
            List<CountResultItem> items = response4.getResult().getCountItems();
            assertEquals(1, items.size());
            CountResultItem item = items.get(0);
            assertEquals(155, item.getID());
            assertEquals(1, item.getCount());
        }
    }

    @Test(expected = DatabaseExistsException.class)
    public void createExistingDatabaseFails() throws IOException {
        try (PilosaClient client = getClient()) {
            client.createDatabase(this.database);
        }

    }

    @Test(expected = FrameExistsException.class)
    public void createExistingFrameFails() throws IOException {
        try (PilosaClient client = getClient()) {
            client.createFrame(this.frame);
        }
    }

    @Test(expected = PilosaException.class)
    public void failedDeleteDatabaseTest() throws IOException {
        try (PilosaClient client = new PilosaClient("http://non-existent-sub.pilosa.com:22222")) {
            client.deleteDatabase(Database.named("non-existent"));
        }
    }

    @Test
    public void ensureDatabaseExistsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Database db = Database.named(this.db.getName() + "-ensure");
            client.ensureDatabaseExists(db);
            client.createFrame(db.frame("frm"));
            client.ensureDatabaseExists(db);  // shouldn't throw an exception
            client.deleteDatabase(db);
        }
    }

    @Test
    public void ensureFrameExistsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Database db = Database.named(this.db.getName() + "-ensure-frame");
            client.createDatabase(db);
            final Frame frame = db.frame("frame");
            client.ensureFrameExists(frame);
            client.ensureFrameExists(frame); // shouldn't throw an exception
            client.query(frame.setBit(1, 10));
            client.deleteDatabase(db);
        }
    }

    @Test
    public void importTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            StaticBitIterator iterator = new StaticBitIterator();
            Frame frame = this.db.frame("importframe");
            client.ensureFrameExists(frame);
            client.importFrame(frame, iterator);
            BatchQuery bq = db.batchQuery(3);
            bq.add(frame.bitmap(2));
            bq.add(frame.bitmap(7));
            bq.add(frame.bitmap(10));
            QueryResponse response = client.query(bq);

            List<Long> target = Arrays.asList(3L, 1L, 5L);
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                BitmapResult br = results.get(i).getBitmap();
                assertEquals(target.get(i), br.getBits().get(0));
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void importFailNot200() throws IOException {
        HttpServer server = runImportFailsHttpServer();
        try (PilosaClient client = new PilosaClient(":15999")) {
            StaticBitIterator iterator = new StaticBitIterator();
            try {
                client.importFrame(this.db.frame("importframe"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void importFail200() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/fragment/nodes");
        try (PilosaClient client = new PilosaClient(":15999")) {
            StaticBitIterator iterator = new StaticBitIterator();
            try {
                client.importFrame(this.db.frame("importframe"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void queryFail200() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/query");
        try (PilosaClient client = new PilosaClient(":15999")) {
            try {
                client.query("somedb", "valid query not required here");
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void fail304EmptyResponse() throws IOException {
        HttpServer server = runContent0HttpServer("/db", 304);
        try (PilosaClient client = new PilosaClient(":15999")) {
            try {
                client.createDatabase(Database.named("foo"));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failQueryEmptyResponse() throws IOException {
        HttpServer server = runContent0HttpServer("/query", 304);
        try (PilosaClient client = new PilosaClient(":15999")) {
            try {
                client.query("foo", "bar");
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failFetchFrameNodesEmptyResponse() throws IOException {
        HttpServer server = runContent0HttpServer("/fragment/nodes", 204);
        try (PilosaClient client = new PilosaClient(":15999")) {
            StaticBitIterator iterator = new StaticBitIterator();
            try {
                client.importFrame(this.db.frame("importframe"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    private PilosaClient getClient() {
        return new PilosaClient(SERVER_ADDRESS);
    }

    private static int counter = 0;

    private static String getRandomDatabaseName() {
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
        @Override
        public void handle(HttpExchange r) throws IOException {
            r.sendResponseHeaders(200, 42);
            OutputStream os = r.getResponseBody();
            os.close();
        }
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
}

class StaticBitIterator implements IBitIterator {
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
