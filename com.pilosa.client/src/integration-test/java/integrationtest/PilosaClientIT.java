package integrationtest;

import com.pilosa.client.*;
import com.pilosa.client.exceptions.PilosaException;
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
    private final static String SERVER_ADDRESS = ":15000";
    private final static String SERVER_PROTOBUF_ADDRESS = "http+pb://:15000";

    @Before
    public void setUp() {
        this.db = getRandomDatabaseName();
    }

    @After
    public void tearDown() {
        PilosaClient client = getClient();
        client.deleteDatabase(this.db);
    }

    @Test
    public void createClientTest() {
        new PilosaClient(new URI(":15000"));
        new PilosaClient(new Cluster());
    }

    @Test
    public void queryTest() {
        PilosaClient client = getClient();
        PilosaResponse response = client.query(db, "SetBit(id=555, frame=\"query-test\", profileID=10)");
        assertEquals(null, response.getResult());
    }

    @Test
    public void queryOverProtobufTest() {
        PilosaClient client = getProtobufClient();
        PilosaResponse response = client.query(db, "SetBit(id=555, frame=\"query-test\", profileID=10)");
        assertEquals(null, response.getResult());
    }

    @Test
    public void queryWithProfilesTest() {
        PilosaClient client = getClient();
        PilosaResponse response = client.queryWithProfiles(db, "Bitmap(id=555, frame=\"query-test\")");
        assertNotNull(response.getResult());
        response = client.queryWithProfiles(db, "Bitmap(id=1, frame=\"another-frame\")");
        assertNotNull(response.getResult());
    }

    @Test
    public void protobufCreateDatabaseDeleteDatabaseTest() {
        final String dbname = "to-be-deleted";
        PilosaClient client = getProtobufClient();
        client.query(dbname, Pql.setBit(1, "delframe", 2));
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
        PilosaClient client = getProtobufClient();
        client.query("testdb", "SetBit(id=5, frame=\"test\", profileID:=10)");
    }

    @Test
    public void ormTest() {
        PilosaClient client = getClient();
        PilosaResponse response;
        Map<String, Object> attrs;
        Map<String, Object> profileAttrs;
        List<Long> bits;
        BitmapResult bitmapResult;
        List<PqlQuery> queryList;

        response = client.query(db, Pql.clearBit(5, "test", 10));
        assertEquals(null, response.getResult());

        response = client.query(db, Pql.setBit(5, "test", 10));
        assertEquals(null, response.getResult());

        response = client.query(db, Pql.setBit(5, "test", 10));
        assertEquals(null, response.getResult());

        attrs = new HashMap<>(0);
        bits = new ArrayList<>(1);
        bits.add(10L);
        response = client.query(db, Pql.bitmap(5, "test"));
        bitmapResult = (BitmapResult)response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        assertNull(response.getProfile());

        // the same with using List<PqlQuery> instead of []PqlQuery
        queryList = new ArrayList<>(1);
        queryList.add(Pql.bitmap(5, "test"));
        response = client.query(db, queryList);
        bitmapResult = (BitmapResult) response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        assertNull(response.getProfile());

        profileAttrs = new HashMap<>(1);
        profileAttrs.put("name", "bombo");
        response = client.query(db, Pql.setProfileAttrs(10, profileAttrs));
        assertNull(response.getResult());

        response = client.queryWithProfiles(db, Pql.bitmap(5, "test"));
        bitmapResult = (BitmapResult) response.getResult();
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
        bitmapResult = (BitmapResult) response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        profile = response.getProfile();
        assertNotNull(profile);
        assertEquals(10, profile.getID());
        assertEquals(profileAttrs, profile.getAttributes());

        response = client.query(db, Pql.clearBit(5, "test", 10));
        assertEquals(null, response.getResult());

        attrs = new HashMap<>(0);
        bits = new ArrayList<>(0);
        response = client.query(db, Pql.bitmap(5, "test"));
        bitmapResult = (BitmapResult)response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());

        client.query(db, Pql.setBit(155, "topn_test", 551));
        response = client.query(db, Pql.topN("topn_test", 1));
        List<CountResultItem> items = (List<CountResultItem>) response.getResult();
        assertEquals(1, items.size());
        CountResultItem item = items.get(0);
        assertEquals(155, item.getKey());
        assertEquals(1, item.getCount());
    }

    @Test
    public void ormProtobufTest() {
        PilosaClient client = getProtobufClient();
        PilosaResponse response;
        Map<String, Object> attrs;
        Map<String, Object> profileAttrs;
        List<Long> bits;
        BitmapResult bitmapResult;
        List<PqlQuery> queryList;

        response = client.query(db, Pql.clearBit(5, "test", 10));
        assertEquals(null, response.getResult());

        response = client.query(db, Pql.setBit(5, "test", 10));
        assertEquals(null, response.getResult());

        response = client.query(db, Pql.setBit(5, "test", 10));
        assertEquals(null, response.getResult());

        attrs = new HashMap<>(0);
        bits = new ArrayList<>(1);
        bits.add(10L);
        response = client.query(db, Pql.bitmap(5, "test"));
        bitmapResult = (BitmapResult) response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        assertNull(response.getProfile());

        // the same with using List<PqlQuery> instead of []PqlQuery
        queryList = new ArrayList<>(1);
        queryList.add(Pql.bitmap(5, "test"));
        response = client.query(db, queryList);
        bitmapResult = (BitmapResult) response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        assertNull(response.getProfile());

        profileAttrs = new HashMap<>(1);
        profileAttrs.put("name", "bombo");
        response = client.query(db, Pql.setProfileAttrs(10, profileAttrs));
        assertNull(response.getResult());

        response = client.queryWithProfiles(db, Pql.bitmap(5, "test"));
        bitmapResult = (BitmapResult) response.getResult();
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
        bitmapResult = (BitmapResult) response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());
        profile = response.getProfile();
        assertNotNull(profile);
        assertEquals(10, profile.getID());
        assertEquals(profileAttrs, profile.getAttributes());

        response = client.query(db, Pql.clearBit(5, "test", 10));
        assertEquals(null, response.getResult());

        attrs = new HashMap<>(0);
        bits = new ArrayList<>(0);
        response = client.query(db, Pql.bitmap(5, "test"));
        bitmapResult = (BitmapResult) response.getResult();
        assertNotNull(bitmapResult);
        assertEquals(attrs, bitmapResult.getAttributes());
        assertEquals(bits, bitmapResult.getBits());

        client.query(db, Pql.setBit(155, "topn_test", 551));
        response = client.query(db, Pql.topN("topn_test", 1));
        List<CountResultItem> items = (List<CountResultItem>) response.getResult();
        assertEquals(1, items.size());
        CountResultItem item = items.get(0);
        assertEquals(155, item.getKey());
        assertEquals(1, item.getCount());
    }

    @Test(expected = PilosaException.class)
    public void failedDeleteDatabaseTest() {
        PilosaClient client = new PilosaClient("http://non-existent-sub.pilosa.com:22222");
        client.deleteDatabase("non-existent");
    }

    @Test
    public void importTest() {
        PilosaClient client = this.getClient();
        StaticBitIterator iterator = new StaticBitIterator();
        client.importFrame("importdb", "importframe", iterator);
        PilosaResponse response = client.query("importdb",
                Pql.bitmap(2, "importframe"),
                Pql.bitmap(7, "importframe"),
                Pql.bitmap(10, "importframe"));

        List<Long> target = Arrays.asList(3L, 1L, 5L);
        List<Object> results = response.getResults();
        for (int i = 0; i < results.size(); i++) {
            BitmapResult br = (BitmapResult) results.get(i);
            assertEquals(target.get(i), br.getBits().get(0));
        }
    }

    @Test(expected = PilosaException.class)
    public void importFailNot200() {
        runImportFailsHttpServer(15999);
        PilosaClient client = new PilosaClient(":15999");
        StaticBitIterator iterator = new StaticBitIterator();
        client.importFrame("importdb", "importframe", iterator);
    }

    private PilosaClient getClient() {
        return new PilosaClient(SERVER_ADDRESS);
    }

    private PilosaClient getProtobufClient() {
        return new PilosaClient(SERVER_PROTOBUF_ADDRESS);
    }

    private static int counter = 0;

    private static String getRandomDatabaseName() {
        return String.format("testdb-%d", ++counter);
    }

    private void runImportFailsHttpServer(int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/fragment/nodes", new FragmentNodesHandler());
            server.setExecutor(null);
            server.start();
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
    }

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
