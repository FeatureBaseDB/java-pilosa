package integrationtest;

import com.pilosa.client.*;
import com.pilosa.client.exceptions.DatabaseExistsException;
import com.pilosa.client.exceptions.FrameExistsException;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.Database;
import com.pilosa.client.orm.Frame;
import com.pilosa.client.orm.Pql;
import com.pilosa.client.orm.PqlQuery;
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
// $ docker run -it --rm --name pilosa -p 15000:15000 pilosa:latest

@Category(IntegrationTest.class)
public class PilosaClientIT {
    private String db;
    private Database database;
    private Frame frame;
    private final static String SERVER_ADDRESS = ":15000";

    @Before
    public void setUp() {
        this.db = getRandomDatabaseName();
        PilosaClient client = getClient();
        client.createDatabase(this.db);
        client.createFrame(this.db, "query-test");
        client.createFrame(this.db, "another-frame");
        client.createFrame(this.db, "test");
        client.createFrame(this.db, "count-test");
        client.createFrame(this.db, "importframe");
        client.createFrame(this.db, "topn_test");

        this.database = Database.named(this.db + "-opts", DatabaseOptions.withColumnLabel("user"));
        client.createDatabase(this.database);
        this.frame = this.database.frame("collab", FrameOptions.withRowLabel("project"));
        client.createFrame(this.frame);
    }

    @After
    public void tearDown() {
        PilosaClient client = getClient();
        client.deleteDatabase(this.db);
        client.deleteDatabase(this.database);
    }

    @Test
    public void createClientTest() {
        new PilosaClient(new URI(":15000"));
        new PilosaClient(new Cluster());
    }

    @Test
    public void queryTest() {
        PilosaClient client = getClient();
        QueryResponse response = client.query(db, "SetBit(id=555, frame=\"query-test\", profileID=10)");
        assertNotNull(response.getResult());
    }

    @Test
    public void queryWithProfilesTest() {
        PilosaClient client = getClient();
        QueryResponse response = client.queryWithProfiles(db, "Bitmap(id=555, frame=\"query-test\")");
        assertNotNull(response.getResult());
        response = client.queryWithProfiles(db, "Bitmap(id=1, frame=\"another-frame\")");
        assertNotNull(response.getResult());
    }

    @Test
    public void protobufCreateDatabaseDeleteDatabaseTest() {
        final String dbname = "to-be-deleted-" + this.db ;
        PilosaClient client = getClient();
        client.createDatabase(dbname);
        client.createFrame(dbname, "delframe");
        client.query(dbname, Pql.setBit(1, "delframe", 2));
        client.deleteDatabase(dbname);
    }

    @Test
    public void createDatabaseWithColumnLabelFrameWithRowLabel() {
        final String dbname = "db-col-label-" + this.db;
        PilosaClient client = getClient();
        client.createDatabase(dbname, DatabaseOptions.withColumnLabel("colz"));
        client.createFrame(dbname, "my-frame", FrameOptions.withRowLabel("rowz"));
        client.deleteDatabase(dbname);
    }

    @Test(expected = PilosaException.class)
    public void failedConnectionTest() {
        PilosaClient client = new PilosaClient("http://non-existent-sub.pilosa.com:22222");
        client.query("test2db", "SetBit(id=15, frame=\"test\", profileID=10)");
    }

    @Test(expected = PilosaException.class)
    public void unknownSchemeTest() {
        PilosaClient client = new PilosaClient("notknown://:15555");
        client.query("test2db", "SetBit(id=15, frame=\"test\", profileID=10)");
    }

    @Test(expected = PilosaException.class)
    public void parseErrorTest() {
        PilosaClient client = getClient();
        client.query("testdb", "SetBit(id=5, frame=\"test\", profileID:=10)");
    }

    @Test(expected = PilosaException.class)
    public void protobufParseErrorTest() {
        PilosaClient client = getClient();
        client.query("testdb", "SetBit(id=5, frame=\"test\", profileID:=10)");
    }

    @Test
    public void ormTest() {
        PilosaClient client = getClient();
        QueryResponse response;
        Map<String, Object> attrs;
        Map<String, Object> profileAttrs;
        List<Long> bits;
        BitmapResult bitmapResult;
        List<PqlQuery> queryList;

        client.query(db, Pql.clearBit(5, "test", 10));
        client.query(db, Pql.setBit(5, "test", 10));
        client.query(db, Pql.setBit(5, "test", 10));

        attrs = new HashMap<>(0);
        bits = new ArrayList<>(1);
        bits.add(10L);
        response = client.query(db, Pql.bitmap(5, "test"));
        bitmapResult = response.getResult().getBitmap();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        assertNull(response.getProfile());

        // the same with using List<PqlQuery> instead of []PqlQuery
        queryList = new ArrayList<>(1);
        queryList.add(Pql.bitmap(5, "test"));
        response = client.query(db, queryList);
        bitmapResult = response.getResult().getBitmap();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        assertNull(response.getProfile());

        profileAttrs = new HashMap<>(1);
        profileAttrs.put("name", "bombo");
        client.query(db, Pql.setProfileAttrs(10, profileAttrs));

        response = client.queryWithProfiles(db, Pql.bitmap(5, "test"));
        bitmapResult = response.getResult().getBitmap();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        ProfileItem profile = response.getProfile();
        assertNotNull(profile);
        assertEquals(10, profile.getID());
        assertEquals(profileAttrs, profile.getAttributes());

        // the same with using List<PqlQuery> instead of []PqlQuery
        queryList = new ArrayList<>(1);
        queryList.add(Pql.bitmap(5, "test"));
        response = client.queryWithProfiles(db, queryList);
        bitmapResult = response.getResult().getBitmap();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        profile = response.getProfile();
        assertNotNull(profile);
        assertEquals(10, profile.getID());
        assertEquals(profileAttrs, profile.getAttributes());

        client.query(db, Pql.clearBit(5, "test", 10));

        attrs = new HashMap<>(0);
        bits = new ArrayList<>(0);
        response = client.query(db, Pql.bitmap(5, "test"));
        bitmapResult = response.getResult().getBitmap();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());

        client.query(db, Pql.setBit(155, "topn_test", 551));
        response = client.query(db, Pql.topN("topn_test", 1));
        List<CountResultItem> items = (List<CountResultItem>) response.getResult().getCountItems();
        assertEquals(1, items.size());
        CountResultItem item = items.get(0);
        assertEquals(155, item.getKey());
        assertEquals(1, item.getCount());
    }

    @Test
    public void ormCountTest() {
        PilosaClient client = getClient();
        client.query(this.db,
                Pql.setBit(10, "count-test", 20),
                Pql.setBit(10, "count-test", 21),
                Pql.setBit(15, "count-test", 25));
        QueryResponse response = client.query(this.db, Pql.count(Pql.bitmap(10, "count-test")));
        assertEquals(2, response.getResult().getCount());
    }

    @Test
    public void newOrmTest() {
        PilosaClient client = getClient();
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
    }

    @Test(expected = DatabaseExistsException.class)
    public void createExistingDatabaseFails() {
        PilosaClient client = getClient();
        client.createDatabase(this.database);
    }

    @Test(expected = FrameExistsException.class)
    public void createExistingFrameFails() {
        PilosaClient client = getClient();
        client.createFrame(this.frame);
    }

    @Test(expected = PilosaException.class)
    public void failedDeleteDatabaseTest() {
        PilosaClient client = new PilosaClient("http://non-existent-sub.pilosa.com:22222");
        client.deleteDatabase("non-existent");
    }

    @Test
    public void ensureDatabaseExistsTest() {
        PilosaClient client = getClient();
        final Database db = Database.named(this.db + "-ensure");
        client.ensureDatabaseExists(db);
        client.createFrame(db.frame("frm"));
        client.ensureDatabaseExists(db);  // shouldn't throw an exception
        client.deleteDatabase(db);
    }

    @Test
    public void ensureFrameExistsTest() {
        PilosaClient client = getClient();
        final Database db = Database.named(this.db + "-ensure-frame");
        client.createDatabase(db);
        final Frame frame = db.frame("frame");
        client.ensureFrameExists(frame);
        client.ensureFrameExists(frame); // shouldn't throw an exception
        client.query(frame.setBit(1, 10));
        client.deleteDatabase(db);

    }

    @Test
    public void importTest() {
        PilosaClient client = this.getClient();
        StaticBitIterator iterator = new StaticBitIterator();
        client.importFrame(this.db, "importframe", iterator);
        QueryResponse response = client.query(this.db,
                Pql.bitmap(2, "importframe"),
                Pql.bitmap(7, "importframe"),
                Pql.bitmap(10, "importframe"));

        List<Long> target = Arrays.asList(3L, 1L, 5L);
        List<QueryResult> results = response.getResults();
        for (int i = 0; i < results.size(); i++) {
            BitmapResult br = results.get(i).getBitmap();
            assertEquals(target.get(i), br.getBits().get(0));
        }
    }

    @Test(expected = PilosaException.class)
    public void importFailNot200() {
        HttpServer server = runImportFailsHttpServer();
        PilosaClient client = new PilosaClient(":15999");
        StaticBitIterator iterator = new StaticBitIterator();
        try {
            client.importFrame(this.db, "importframe", iterator);
        } finally {
            if (server != null) {
                server.stop(0);
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void importFail200() {
        HttpServer server = runContentSizeLyingHttpServer("/fragment/nodes");
        PilosaClient client = new PilosaClient(":15999");
        StaticBitIterator iterator = new StaticBitIterator();
        try {
            client.importFrame(this.db, "importframe", iterator);
        } finally {
            if (server != null) {
                server.stop(0);
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void queryFail200() {
        HttpServer server = runContentSizeLyingHttpServer("/query");
        PilosaClient client = new PilosaClient(":15999");
        try {
            client.query("somedb", "valid query not required here");
        } finally {
            if (server != null) {
                server.stop(0);
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

//    private HttpServer runFrameNode

    static class FragmentNodesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange r) throws IOException {
            String response = "[{\"host\":\"localhost:15999\"}]";
            r.sendResponseHeaders(200, response.length());
            OutputStream os = r.getResponseBody();
            os.write(response.getBytes());
            os.close();
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
