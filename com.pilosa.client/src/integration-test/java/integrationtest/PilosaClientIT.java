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
import com.pilosa.client.csv.*;
import com.pilosa.client.exceptions.HttpConflict;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.orm.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

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
    private Index keyIndex;
    private Field field;
    private Field keyField;

    @Before
    public void setUp() throws IOException {
        try (PilosaClient client = getClient()) {
            this.schema = client.readSchema();
            this.index = schema.index(getRandomIndexName());
            this.index.field("another-field");
            this.index.field("test");
            this.index.field("count-test");
            this.index.field("topn_test");

            this.colIndex = schema.index(this.index.getName() + "-opts");
            FieldOptions fieldOptions = FieldOptions.withDefaults();
            this.field = this.colIndex.field("collab", fieldOptions);
            fieldOptions = FieldOptions.builder()
                    .setKeys(true)
                    .build();
            this.keyField = this.index.field("index-key-field", fieldOptions);

            IndexOptions indexOptions = IndexOptions.builder()
                    .setKeys(true)
                    .build();
            this.keyIndex = schema.index("key-index", indexOptions);

            client.syncSchema(this.schema);
        }
    }

    @After
    public void tearDown() throws IOException {
        try (PilosaClient client = getClient()) {
            client.deleteIndex(this.index);
            client.deleteIndex(this.colIndex);
            client.deleteIndex(this.keyIndex);
        } catch (PilosaException ex) {
            // pass
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
    public void responseDefaultsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            QueryResponse response = client.query(this.field.topN(5));
            assertNotNull(response.getResult().getRow());
            assertNotNull(response.getResult().getCountItems());
            response = client.query(this.field.row(99999));
            assertNotNull(response.getResult().getRow());
            assertNotNull(response.getResult().getCountItems());
        }

    }

    @Test
    public void createFieldWithTimeQuantumTest() throws IOException {
        FieldOptions options = FieldOptions.builder()
                .fieldTime(TimeQuantum.YEAR_MONTH_DAY)
                .build();
        Field field = this.index.field("field-with-timequantum", options);
        try (PilosaClient client = getClient()) {
            client.ensureField(field);
            Schema schema = client.readSchema();
            Field info = findField(schema, field);
            assertNotNull(info);
            assertEquals(TimeQuantum.YEAR_MONTH_DAY, info.getOptions().getTimeQuantum());
        }
    }

    @Test
    public void testSchema() throws IOException {
        try (PilosaClient client = getClient()) {
            Schema schema = client.readSchema();
            assertTrue(schema.getIndexes().size() >= 1);
            assertTrue(schema.getIndexes().entrySet().iterator().next().getValue().getFields().size() >= 1);
            FieldOptions fieldOptions = FieldOptions.builder()
                    .fieldSet(CacheType.LRU, 9999)
                    .build();
            Field field = this.index.field("schema-test-field", fieldOptions);
            client.ensureField(field);
            schema = client.readSchema();
            Field f = schema.getIndexes().get(this.index.getName()).getFields().get("schema-test-field");
            FieldOptions fo = f.getOptions();
            assertEquals(FieldType.SET, fo.getFieldType());
            assertEquals(9999, fo.getCacheSize());
            assertEquals(CacheType.LRU, fo.getCacheType());

            fieldOptions = FieldOptions.builder()
                    .fieldInt(-10, 10)
                    .build();
            field = this.index.field("schema-test-field-int", fieldOptions);
            client.ensureField(field);
            schema = client.readSchema();
            f = schema.getIndexes().get(this.index.getName()).getFields().get("schema-test-field-int");
            fo = f.getOptions();
            assertEquals(FieldType.INT, fo.getFieldType());
            assertEquals(-10, fo.getMin());
            assertEquals(10, fo.getMax());

            fieldOptions = FieldOptions.builder()
                    .fieldTime(TimeQuantum.YEAR_MONTH_DAY)
                    .build();
            field = this.index.field("schema-test-field-time", fieldOptions);
            client.ensureField(field);
            schema = client.readSchema();
            f = schema.getIndexes().get(this.index.getName()).getFields().get("schema-test-field-time");
            fo = f.getOptions();
            assertEquals(FieldType.TIME, fo.getFieldType());
            assertEquals(TimeQuantum.YEAR_MONTH_DAY, fo.getTimeQuantum());
        }
    }

    @Test
    public void queryTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Field field = this.index.field("query-test");
            client.ensureField(field);
            QueryResponse response = client.query(field.set(555, 10));
            assertNotNull(response.getResult());
        }
    }

    @Test
    public void queryWithColumnsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Field field = this.index.field("query-test");
            client.ensureField(field);
            client.query(field.set(100, 1000));
            Map<String, Object> columnAttrs = new HashMap<>(1);
            columnAttrs.put("name", "bombo");
            client.query(this.index.setColumnAttrs(1000, columnAttrs));
            QueryOptions queryOptions = QueryOptions.builder()
                    .setColumnAttributes(true)
                    .build();
            QueryResponse response = client.query(field.row(100), queryOptions);
            assertNotNull(response.getColumn());
            assertEquals(1000, response.getColumn().getID());
            assertEquals(columnAttrs, response.getColumn().getAttributes());

            response = client.query(field.row(300));
            assertNull(response.getColumn());
        }
    }

    @Test
    public void protobufCreateIndexDeleteIndexTest() throws IOException {
        final Index indexName = Index.create("to-be-deleted-" + this.index.getName());
        Field field = indexName.field("delfield");
        try (PilosaClient client = getClient()) {
            try {
                client.createIndex(indexName);
                client.createField(field);
                client.query(field.set(1, 2));
            } finally {
                client.deleteIndex(indexName);
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failedConnectionTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("http://non-existent-sub.pilosa.com:22222")) {
            client.query(this.field.set(15, 10));
        }
    }

    @Test(expected = PilosaException.class)
    public void unknownSchemeTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("notknown://:15555")) {
            client.query(this.field.set(15, 10));
        }
    }

    @Test(expected = PilosaException.class)
    public void parseErrorTest() throws IOException {
        try (PilosaClient client = getClient()) {
            QueryResponse response = client.query(this.index.rawQuery("SetBit(id=5, field=\"test\", col_id:=10)"));
            System.out.println(response);
        }
    }

    @Test
    public void ormCountTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Field countField = this.index.field("count-test");
            client.ensureField(countField);
            PqlBatchQuery qry = this.index.batchQuery();
            qry.add(countField.set(10, 20));
            qry.add(countField.set(10, 21));
            qry.add(countField.set(15, 25));
            client.query(qry);
            QueryResponse response = client.query(this.index.count(countField.row(10)));
            assertEquals(2, response.getResult().getCount());
        }
    }

    @Test
    public void newOrmTest() throws IOException {
        try (PilosaClient client = getClient()) {
            client.query(this.colIndex.batchQuery(
                    this.field.set(5, 20),
                    this.field.set(10, 20),
                    this.field.set(10, 30)));
            QueryResponse response1 = client.query(this.field.row(10));
            assertEquals(0, response1.getColumns().size());
            RowResult row1 = response1.getResult().getRow();
            assertEquals(0, row1.getAttributes().size());
            assertEquals(2, row1.getColumns().size());
            assertEquals(20, (long) row1.getColumns().get(0));

            Map<String, Object> columnAttrs = new HashMap<>(1);
            columnAttrs.put("name", "bombo");
            client.query(this.colIndex.setColumnAttrs(20, columnAttrs));
            QueryOptions queryOptions = QueryOptions.builder()
                    .setColumnAttributes(true)
                    .build();
            QueryResponse response2 = client.query(this.field.row(10), queryOptions);
            ColumnItem column = response2.getColumn();
            assertNotNull(column);
            assertEquals(20, column.getID());

            Map<String, Object> rowAttrs = new HashMap<>(1);
            rowAttrs.put("active", true);
            rowAttrs.put("unsigned", 5);
            rowAttrs.put("height", 1.81);
            rowAttrs.put("name", "Mr. Pi");
            client.query(this.field.setRowAttrs(10, rowAttrs));
            QueryResponse response3 = client.query(this.field.row(10));
            RowResult row = response3.getResult().getRow();
            assertEquals(2, row.getColumns().size());
            assertEquals(4, row.getAttributes().size());
            assertEquals(true, row.getAttributes().get("active"));
            assertEquals(5L, row.getAttributes().get("unsigned"));
            assertEquals(1.81, row.getAttributes().get("height"));
            assertEquals("Mr. Pi", row.getAttributes().get("name"));

            client.httpRequest("POST", "/recalculate-caches");

            Field topnField = this.index.field("topn_test");
            client.query(topnField.set(155, 551));
            QueryResponse response4 = client.query(topnField.topN(1));
            List<CountResultItem> items = response4.getResult().getCountItems();
            assertEquals(1, items.size());
            CountResultItem item = items.get(0);
            assertEquals(155, item.getID());
            assertEquals(1, item.getCount());

            QueryResponse response5 = client.query(this.field.topN(10, null, "name", "Mr. Pi"));
            items = response5.getResult().getCountItems();
            assertEquals(1, items.size());
            assertEquals(10, items.get(0).getID());
            assertEquals(2, items.get(0).getCount());
        }
    }

    @Test
    public void testTopN() throws IOException, InterruptedException {
        try (PilosaClient client = getClient()) {
            Field field = this.index.field("topn_test");
            client.ensureField(field);
            client.query(this.index.batchQuery(
                    field.set(10, 5),
                    field.set(10, 10),
                    field.set(10, 15),
                    field.set(20, 5),
                    field.set(30, 5)
            ));
            // The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
            client.httpRequest("POST", "/recalculate-caches");
            QueryResponse response = client.query(field.topN(2));
            List<CountResultItem> items = response.getResult().getCountItems();
            assertEquals(2, items.size());
            CountResultItem item = items.get(0);
            assertEquals(10, item.getID());
            assertEquals(3, item.getCount());
        }
    }

    @Test
    public void testKeys() throws IOException {
        try (PilosaClient client = getClient()) {
            FieldOptions options = FieldOptions.builder()
                    .setKeys(true)
                    .build();
            Field field = this.keyIndex.field("keys-test", options);
            client.syncSchema(this.schema);
            client.query(field.set("stringRow", "stringCol"));
            QueryResponse response = client.query(field.row("stringRow"));
            List<String> target = new ArrayList<>();
            target.add("stringCol");
            assertEquals(target, response.getResult().getRow().getKeys());
        }
    }

    @Test
    public void testNot() throws IOException {
        IndexOptions options = IndexOptions.builder()
                .setTrackExistence(true)
                .build();
        Index index = this.schema.index("not-test", options);
        Field field = index.field("f1");
        try (PilosaClient client = getClient()) {
            client.syncSchema(this.schema);
            try {
                client.query(index.batchQuery(
                        field.set(1, 10),
                        field.set(1, 11),
                        field.set(2, 11),
                        field.set(2, 12),
                        field.set(2, 13)
                ));
                QueryResponse response = client.query(index.not(field.row(1)));
                List<Long> target = new ArrayList<>();
                target.add(12L);
                target.add(13L);
                assertEquals(target, response.getResult().getRow().getColumns());
            } finally {
                client.deleteIndex(index);
            }
        }
    }

    @Test
    public void testStore() throws IOException {
        Index index = this.schema.index("store-test");
        Field fromField = index.field("from-field");
        Field toField = index.field("to-field");
        try (PilosaClient client = getClient()) {
            client.syncSchema(this.schema);
            try {
                client.query(index.batchQuery(
                        fromField.set(10, 100),
                        fromField.set(10, 200),
                        toField.store(fromField.row(10), 1)
                ));
                QueryResponse response = client.query(toField.row(1));
                List<Long> target = new ArrayList<>();
                target.add(100L);
                target.add(200L);
                assertEquals(target, response.getResult().getRow().getColumns());
            } finally {
                client.deleteIndex(index);
            }
        }
    }

    @Test
    public void testGroupBy() throws IOException {
        Field field = index.field("groupby-field");
        try (PilosaClient client = getClient()) {
            client.createField(field);
            client.query(index.batchQuery(
                    field.set(1, 100),
                    field.set(1, 200),
                    field.set(2, 200)
            ));
            QueryResponse response = client.query(index.groupBy(field.rows()));

            List<FieldRow> fieldRows1 = Collections.singletonList(FieldRow.create("groupby-field", 1));
            List<FieldRow> fieldRows2 = Collections.singletonList(FieldRow.create("groupby-field", 2));
            List<GroupCount> target = Arrays.asList(
                    GroupCount.create(fieldRows1, 2),
                    GroupCount.create(fieldRows2, 1));
            assertEquals(target, response.getResult().getGroupCounts());
        }
    }

    @Test(expected = PilosaException.class)
    public void queryFailsWithError() throws IOException {
        try (PilosaClient client = getClient()) {
            client.query(this.index.rawQuery("invalid query"));
        }
    }

    @Test(expected = HttpConflict.class)
    public void createExistingDatabaseFails() throws IOException {
        try (PilosaClient client = getClient()) {
            client.createIndex(this.colIndex);
        }

    }

    @Test(expected = HttpConflict.class)
    public void createExistingFieldFails() throws IOException {
        try (PilosaClient client = getClient()) {
            client.createField(this.field);
        }
    }

    @Test(expected = PilosaException.class)
    public void failedDeleteIndexTest() throws IOException {
        try (PilosaClient client = PilosaClient.withAddress("http://non-existent-sub.pilosa.com:22222")) {
            client.deleteIndex(Index.create("non-existent"));
        }
    }

    @Test
    public void ensureIndexExistsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Index index = Index.create(this.index.getName() + "-ensure");
            client.ensureIndex(index);
            client.createField(index.field("frm"));
            client.ensureIndex(index);  // shouldn't throw an exception
            client.deleteIndex(index);
        }
    }

    @Test
    public void ensureFieldExistsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Index index = Index.create(this.index.getName() + "-ensure-field");
            try {
                client.createIndex(index);
                final Field field = index.field("field");
                client.ensureField(field);
                client.ensureField(field); // shouldn't throw an exception
                client.query(field.set(1, 10));
            } finally {
                client.deleteIndex(index);
            }
        }
    }

    @Test
    public void deleteFieldTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final Field field = index.field("to-delete");
            client.ensureField(field);
            client.deleteField(field);
            // the following should succeed
            client.createField(field);
        }
    }

    @Test
    public void importRowIDColumnIDTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            LineDeserializer deserializer = new RowIDColumnIDDeserializer();
            RecordIterator iterator = csvRecordIterator("row_id-column_id.csv", deserializer);
            Field field = this.index.field("importfield-rowid-colid");
            client.ensureField(field);
            client.importField(field, iterator);
            PqlBatchQuery bq = index.batchQuery(
                    field.row(1L),
                    field.row(5L),
                    field.row(3L)
            );
            QueryResponse response = client.query(bq);

            List<Long> target = Arrays.asList(10L, 20L, 41L);
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                RowResult br = results.get(i).getRow();
                assertEquals(target.get(i), br.getColumns().get(0));
            }
        }
    }

    @Test
    public void importRowIDColumnKeyTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            LineDeserializer deserializer = new RowIDColumnKeyDeserializer();
            RecordIterator iterator = csvRecordIterator("row_id-column_key.csv", deserializer);
            Field field = this.keyIndex.field("importfield-rowid-colkey");
            client.ensureField(field);
            client.importField(field, iterator);
            PqlBatchQuery bq = this.keyIndex.batchQuery(
                    field.row(1L),
                    field.row(5L),
                    field.row(3L)
            );
            QueryResponse response = client.query(bq);

            List<String> target = Arrays.asList("ten", "twenty", "forty-one");
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                RowResult br = results.get(i).getRow();
                assertEquals(target.get(i), br.getKeys().get(0));
            }
        }
    }

    @Test
    public void importRowKeyColumnIDTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            LineDeserializer deserializer = new RowKeyColumnIDDeserializer();
            RecordIterator iterator = csvRecordIterator("row_key-column_id.csv", deserializer);
            FieldOptions fieldOptions = FieldOptions.builder()
                    .setKeys(true)
                    .build();
            Field field = this.index.field("importfield-rowkey-colid", fieldOptions);
            client.ensureField(field);
            client.importField(field, iterator);
            PqlBatchQuery bq = index.batchQuery(
                    field.row("one"),
                    field.row("five"),
                    field.row("three")
            );
            QueryResponse response = client.query(bq);

            List<Long> target = Arrays.asList(10L, 20L, 41L);
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                RowResult br = results.get(i).getRow();
                assertEquals(target.get(i), br.getColumns().get(0));
            }
        }
    }

    @Test
    public void importRowKeyColumnKeyTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            LineDeserializer deserializer = new RowKeyColumnKeyDeserializer();
            RecordIterator iterator = csvRecordIterator("row_key-column_key.csv", deserializer);
            FieldOptions options = FieldOptions.builder()
                    .setKeys(true)
                    .build();
            Field field = this.keyIndex.field("importfield-rowkey-columnkey", options);
            client.ensureField(field);
            client.importField(field, iterator);
            PqlBatchQuery bq = keyIndex.batchQuery(
                    field.row("one"),
                    field.row("five"),
                    field.row("three")
            );
            QueryResponse response = client.query(bq);

            List<String> target = Arrays.asList("ten", "twenty", "forty-one");
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                RowResult br = results.get(i).getRow();
                assertEquals(target.get(i), br.getKeys().get(0));
            }
        }
    }

    @Test
    public void importRoaringTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            RecordIterator iterator = StaticColumnIterator.columnsWithIDs();
            Field field = this.index.field("importfield-roaring");
            client.ensureField(field);
            ImportOptions importOptions = ImportOptions.builder()
                    .setRoaring(true)
                    .build();
            client.importField(field, iterator, importOptions);
            PqlBatchQuery bq = index.batchQuery(
                    field.row(2),
                    field.row(7),
                    field.row(10)
            );
            QueryResponse response = client.query(bq);

            List<Long> target = Arrays.asList(3L, 1L, 5L);
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                RowResult br = results.get(i).getRow();
                assertEquals(target.get(i), br.getColumns().get(0));
            }

            // test clear import
            iterator = StaticColumnIterator.columnsWithIDs();
            importOptions = ImportOptions.builder()
                    .setRoaring(true)
                    .setClear(true)
                    .build();
            client.importField(field, iterator, importOptions);
            bq = index.batchQuery(
                    field.row(2),
                    field.row(7),
                    field.row(10)
            );
            response = client.query(bq);
            target = new ArrayList<>(0);
            results = response.getResults();
            for (QueryResult result : results) {
                RowResult br = result.getRow();
                assertEquals(target, br.getColumns());
            }
        }
    }


    @Test
    public void importRoaringTimeFieldTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            RecordIterator iterator = StaticColumnIteratorWithTimestamp.columnsWithIDs();
            FieldOptions fieldOptions = FieldOptions.builder()
                    .fieldTime(TimeQuantum.YEAR_MONTH_DAY_HOUR)
                    .build();
            Field field = this.index.field("importfield-roaring-time", fieldOptions);
            client.ensureField(field);
            ImportOptions importOptions = ImportOptions.builder()
                    .setRoaring(true)
                    .build();
            client.importField(field, iterator, importOptions);
            PqlBatchQuery bq = index.batchQuery(
                    field.row(2),
                    field.row(7),
                    field.row(10)
            );
            QueryResponse response = client.query(bq);

            List<Long> target = Arrays.asList(3L, 1L, 5L);
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                RowResult br = results.get(i).getRow();
                assertEquals(target.get(i), br.getColumns().get(0));
            }

            target = Arrays.asList(5L);
            Calendar start = new GregorianCalendar(2016, 1, 1, 0, 0, 0);
            Calendar end = new GregorianCalendar(2019, 1, 1, 0, 0, 0);
            response = client.query(field.row(10, start.getTime(), end.getTime()));
            assertEquals(target, response.getResult().getRow().getColumns());

            // test clear import
            iterator = StaticColumnIterator.columnsWithIDs();
            importOptions = ImportOptions.builder()
                    .setRoaring(true)
                    .setClear(true)
                    .build();
            client.importField(field, iterator, importOptions);
            bq = index.batchQuery(
                    field.row(2),
                    field.row(7),
                    field.row(10)
            );
            response = client.query(bq);
            target = new ArrayList<>(0);
            results = response.getResults();
            for (QueryResult result : results) {
                RowResult br = result.getRow();
                assertEquals(target, br.getColumns());
            }
        }
    }


    @Test
    public void importFieldValuesTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            RecordIterator iterator = StaticColumnIterator.fieldValuesWithIDs();
            FieldOptions options = FieldOptions.builder()
                    .fieldInt(0, 100)
                    .build();
            Field field = this.index.field("importvaluefield", options);
            client.ensureField(field);
            client.importField(field, iterator);

            Field field2 = this.index.field("importvaluefield-set");
            client.ensureField(field2);
            PqlBatchQuery bq = this.index.batchQuery(
                    field2.set(1, 10),
                    field2.set(1, 7)
            );
            client.query(bq);

            QueryResponse response = client.query(field.sum(field2.row(1)));
            assertEquals(8, response.getResult().getValue());

            // test clear import
            iterator = StaticColumnIterator.fieldValuesWithIDs();
            ImportOptions importOptions = ImportOptions.builder()
                    .setClear(true)
                    .build();
            client.importField(field, iterator, importOptions);
            response = client.query(field.sum(field2.row(1)));
            assertEquals(0, response.getResult().getValue());
        }
    }

    @Test
    public void importFieldValuesWithKeysTest() throws IOException {
        try (PilosaClient client = this.getClient()) {
            RecordIterator iterator = StaticColumnIterator.fieldValuesWithKeys();
            FieldOptions options = FieldOptions.builder()
                    .fieldInt(0, 100)
                    .setKeys(true)
                    .build();
            Field field = this.keyIndex.field("importvaluefieldkeys", options);
            client.ensureField(field);
            client.importField(field, iterator);

            FieldOptions options2 = FieldOptions.builder()
                    .setKeys(true)
                    .build();
            Field field2 = this.keyIndex.field("importvaluefieldkeys-set", options2);
            client.ensureField(field2);
            PqlBatchQuery bq = this.keyIndex.batchQuery(
                    field2.set("one", "ten"),
                    field2.set("one", "seven")
            );
            client.query(bq);

            QueryResponse response = client.query(field.sum(field2.row("one")));
            assertEquals(8, response.getResult().getValue());
        }
    }

    @Test
    public void importTestWithBatch() throws IOException {
        try (PilosaClient client = this.getClient()) {
            RecordIterator iterator = StaticColumnIterator.columnsWithIDs();
            Field field = this.index.field("importfield");
            client.ensureField(field);
            ImportOptions options = ImportOptions.builder().
                    setStrategy(ImportOptions.Strategy.BATCH).
                    setBatchSize(3).
                    setThreadCount(1).
                    build();

            client.importField(field, iterator, options);
            PqlBatchQuery bq = index.batchQuery(
                    field.row(2),
                    field.row(7),
                    field.row(10)
            );
            QueryResponse response = client.query(bq);

            List<Long> target = Arrays.asList(3L, 1L, 5L);
            List<QueryResult> results = response.getResults();
            for (int i = 0; i < results.size(); i++) {
                RowResult br = results.get(i).getRow();
                assertEquals(target.get(i), br.getColumns().get(0));
            }

            // test clear imports
            options = ImportOptions.builder()
                    .setClear(true)
                    .build();
            iterator = StaticColumnIterator.columnsWithIDs();
            client.importField(field, iterator, options);
            bq = index.batchQuery(
                    field.row(2),
                    field.row(7),
                    field.row(10)
            );
            response = client.query(bq);
            results = response.getResults();
            target = new ArrayList<>(0);
            for (QueryResult result : results) {
                RowResult br = result.getRow();
                assertEquals(target, br.getColumns());
            }
        }
    }

    @Test
    public void importTest2() throws IOException {
        class ImportMonitor implements Runnable {
            @Override
            public void run() {
                while (true) {
                    try {
                        ImportStatusUpdate statusUpdate = this.statusQueue.take();
                        assertEquals(String.format("thread:%d imported:%d columns for shard:%d in:%d ms",
                                statusUpdate.getThreadID(), statusUpdate.getImportedCount(), statusUpdate.getShard(), statusUpdate.getTimeMs()),
                                statusUpdate.toString()); // for coverage
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }

            ImportMonitor(final BlockingQueue<ImportStatusUpdate> statusQueue) {
                this.statusQueue = statusQueue;
            }

            private final BlockingQueue<ImportStatusUpdate> statusQueue;
        }

        try (PilosaClient client = this.getClient()) {
            long maxID = 300_000_000;
            long maxColumns = 100_000;

            BlockingQueue<ImportStatusUpdate> statusQueue = new LinkedBlockingDeque<>(1000);
            ImportMonitor monitor = new ImportMonitor(statusQueue);
            Thread monitorThread = new Thread(monitor);
            monitorThread.setDaemon(true);
            monitorThread.start();

            RecordIterator iterator = new XColumnIterator(maxID, maxColumns);

            Field field = this.index.field("importfield2");
            client.ensureField(field);

            ImportOptions options = ImportOptions.builder()
                    .setBatchSize(100000)
                    .setThreadCount(2)
                    .setStrategy(ImportOptions.Strategy.TIMEOUT)
                    .setTimeoutMs(5)
                    .build();
            client.importField(field, iterator, options, statusQueue);
            monitorThread.interrupt();
        }
    }

    @Test
    public void workerInterruptedTest() throws IOException {
        class Importer implements Runnable {
            Importer(PilosaClient client, Field field) {
                this.client = client;
                this.field = field;
            }

            @Override
            public void run() {
                RecordIterator iterator = new XColumnIterator(1_000, 1_000);
                BlockingQueue<ImportStatusUpdate> statusQueue = new LinkedBlockingDeque<>(1);

                Field field = this.field;
                this.client.ensureField(field);

                ImportOptions options = ImportOptions.builder()
                        .setStrategy(ImportOptions.Strategy.BATCH)
                        .setBatchSize(500)
                        .setThreadCount(1)
                        .build();
                this.client.importField(field, iterator, options, statusQueue);
            }

            PilosaClient client;
            Field field;
        }

        try (PilosaClient client = this.getClient()) {
            Field field = this.index.field("importfield-queue-interrupt");
            client.ensureField(field);
            Thread importer = new Thread(new Importer(client, field));
            importer.setDaemon(true);
            importer.start();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                fail("interruption was not expected here");
            }
            importer.interrupt();
        }
    }

    @Test
    public void workerImportRemainingColumnsInterruptedTest() throws IOException {
        class Importer implements Runnable {
            Importer(PilosaClient client, Field field) {
                this.client = client;
                this.field = field;
            }

            @Override
            public void run() {
                RecordIterator iterator = new XColumnIterator(1_000, 1_500);
                // There should be 10_000/3_000 == 3 batch status updates
                // And another one for the remaining records
                // Block after the 3 so we have a chance to interrupt the worker thread
                //  while importing remaining records...
                BlockingQueue<ImportStatusUpdate> statusQueue = new LinkedBlockingDeque<>(1);

                Field field = this.field;
                this.client.ensureField(field);

                ImportOptions options = ImportOptions.builder()
                        .setStrategy(ImportOptions.Strategy.BATCH)
                        .setBatchSize(1_000)
                        .setThreadCount(1)
                        .build();
                this.client.importField(field, iterator, options, statusQueue);
            }

            PilosaClient client;
            Field field;
        }

        try (PilosaClient client = this.getClient()) {
            Field field = this.index.field("importfield-queue-interrupt");
            client.ensureField(field);
            Thread importer = new Thread(new Importer(client, field));
//            importer.setDaemon(true);
            importer.start();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                fail("interruption was not expected here");
            }
            importer.interrupt();
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
            client.deleteIndex(this.keyIndex);
            Schema schema = client.readSchema();
            assertEquals(0, schema.getIndexes().size());
        }
    }

    @Test
    public void syncSchemaTest() throws IOException {
        Index index = null;

        try (PilosaClient client = this.getClient()) {
            Schema schema = client.readSchema();
            IndexOptions indexOptions = IndexOptions.builder()
                    .setKeys(true)
                    .setTrackExistence(true)
                    .build();
            index = schema.index("index11", indexOptions);
            FieldOptions fieldOptions = FieldOptions.builder()
                    .setKeys(true)
                    .fieldSet(CacheType.RANKED, 50000)
                    .build();
            index.field("index11-f1", fieldOptions);
            client.syncSchema(schema);

            Schema schema2 = client.readSchema();
            Index index2 = schema2.index("index11");
            assertEquals(index, index2);

            Schema schema3 = Schema.defaultSchema();
            client.syncSchema(schema3);
            Index index3 = schema3.index("index11");
            assertEquals(index, index3);

            Schema schema4 = Schema.defaultSchema();
            Index index4 = schema4.index("index11", indexOptions);
            client.syncSchema(schema4);
            assertEquals(index, index4);


        } finally {
            try (PilosaClient client = this.getClient()) {
                if (index != null) {
                    client.deleteIndex(index);
                }
            }
        }
    }

    @Test
    public void rangeFieldTest() throws IOException {
        try (PilosaClient client = getClient()) {
            FieldOptions options = FieldOptions.builder()
                    .fieldInt(10, 20)
                    .build();
            Field field = this.index.field("rangefield", options);
            client.ensureField(field);
            Field field2 = this.index.field("rangefield-test");
            client.ensureField(field2);
            client.query(this.index.batchQuery(
                    field2.set(1, 10),
                    field2.set(1, 100),
                    field.setValue(10, 11),
                    field.setValue(100, 15)
            ));
            QueryResponse response = client.query(field.sum(field2.row(1)));
            assertEquals(26, response.getResult().getValue());
            assertEquals(2, response.getResult().getCount());

            response = client.query(field.min());
            assertEquals(11, response.getResult().getValue());
            assertEquals(1, response.getResult().getCount());

            response = client.query(field.min(field2.row(1)));
            assertEquals(11, response.getResult().getValue());
            assertEquals(1, response.getResult().getCount());

            response = client.query(field.max());
            assertEquals(15, response.getResult().getValue());
            assertEquals(1, response.getResult().getCount());

            response = client.query(field.max(field2.row(1)));
            assertEquals(15, response.getResult().getValue());
            assertEquals(1, response.getResult().getCount());

            response = client.query(field.lessThan(15));
            assertEquals(1, response.getResults().size());
            assertEquals(10, (long) response.getResult().getRow().getColumns().get(0));
        }
    }

    @Test
    public void excludeAttrsColumnsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            Map<String, Object> attrs = new HashMap<>(1);
            attrs.put("foo", "bar");
            client.query(colIndex.batchQuery(
                    field.set(1, 100),
                    field.setRowAttrs(1, attrs)
            ));

            QueryResponse response;
            QueryOptions options;

            // test exclude records.
            options = QueryOptions.builder()
                    .setExcludeColumns(true)
                    .build();
            response = client.query(field.row(1), options);
            assertEquals(0, response.getResult().getRow().getColumns().size());
            assertEquals(1, response.getResult().getRow().getAttributes().size());

            // test exclude attributes.
            options = QueryOptions.builder()
                    .setExcludeAttributes(true)
                    .build();
            response = client.query(field.row(1), options);
            assertEquals(1, response.getResult().getRow().getColumns().size());
            assertEquals(0, response.getResult().getRow().getAttributes().size());

        }
    }

    @Test
    public void httpRequestTest() throws IOException {
        try (PilosaClient client = getClient()) {
            client.httpRequest("GET", "/status");
        }
    }

    @Test
    public void shardsTest() throws IOException {
        try (PilosaClient client = getClient()) {
            final long shardWidth = ClientOptions.DEFAULT_SHARD_WIDTH;
            client.query(colIndex.batchQuery(
                    field.set(1, 100),
                    field.set(1, shardWidth),
                    field.set(1, shardWidth * 3)
            ));

            QueryOptions options = QueryOptions.builder()
                    .setShards(0L, 3L)
                    .build();
            QueryResponse response = client.query(field.row(1), options);

            List<Long> columns = response.getResult().getRow().getColumns();
            assertEquals(2, columns.size());
            assertEquals(100, (long) columns.get(0));
            assertEquals(shardWidth * 3, (long) columns.get(1));
        }
    }

    @Test
    public void warningResponseTest() throws IOException, InterruptedException {
        String path = String.format("/index/%s/query", this.colIndex.getName());
        String msg = "299 pilosa/2.0 \"Deprecated PQL version: PQL v2 will remove support for SetBit() in Pilosa 2.1. Please update your client to support Set() (See https://docs.pilosa.com/pql#versioning).\" \"Sat, 25 Aug 2019 23:34:45 GMT\"";
        HttpServer server = warningResponseHttpServer(path, msg);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.field.row(1));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void importFailNot200() throws IOException {
        HttpServer server = runImportFailsHttpServer();
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            RecordIterator iterator = StaticColumnIterator.columnsWithIDs();
            try {
                client.importField(this.index.field("importfield"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test
    public void importFail200Test() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/internal/fragment/nodes");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            RecordIterator iterator = StaticColumnIterator.columnsWithIDs();
            try {
                client.importField(this.index.field("importfield"), iterator);
            } catch (PilosaException ex) {
                // pass
                return;
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
        fail("Expected PilosaException to be thrown");
    }

    @Test(expected = PilosaException.class)
    public void queryFail404Test() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/404");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.field.set(15, 10));
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
                client.createIndex(Index.create("foo"));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failQueryEmptyResponseTest() throws IOException {
        String path = String.format("/index/%s/query", this.field.getIndex().getName());
        HttpServer server = runContent0HttpServer(path, 304);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.field.set(15, 10));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failFetchFieldNodesEmptyResponseTest() throws IOException {
        HttpServer server = runContent0HttpServer("/internal/fragment/nodes", 204);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            RecordIterator iterator = StaticColumnIterator.columnsWithIDs();
            try {
                client.importField(this.index.field("importfield"), iterator);
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failFetchCoordinatorNodeEmptyResponseTest() throws IOException {
        HttpServer server = runContent0HttpServer("/status", 204);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.keyField.set("foo", 1));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failFetchCoordinator200Test() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/status");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.keyField.set("foo", 1));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failSchemaEmptyResponseTest() throws IOException {
        HttpServer server = runContent0HttpServer("/schema", 204);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.readServerSchema();
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failSchema200Test() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer("/schema");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.readServerSchema();
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failSchema400IOError() throws IOException {
        HttpServer server = runContentSizeLyingHttpServer400("/schema");
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.readServerSchema();
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
            c.addHost(URI.address(String.format("n%d.nonexistent-improbable:5000", i)));
        }
        PilosaClient client = PilosaClient.withCluster(c);
        client.readServerSchema();
    }

    @Test(expected = PilosaException.class)
    public void failOverCoordinatorTest() throws IOException, InterruptedException {
        HttpServer server = runNonexistentCoordinatorHttpServer();
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                Map<String, Object> attrs = new HashMap<>(1);
                attrs.put("foo", "bar");
                client.query(this.keyIndex.setColumnAttrs("foo", attrs));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = RuntimeException.class)
    public void invalidPilosaClientFails() throws IOException {
        Cluster cluster = Cluster.withHost(URI.address(getBindAddress()));
        ClientOptions options = ClientOptions.builder().build();
        try (PilosaClient client = new InvalidPilosaClient(cluster, options)) {
            RecordIterator iterator = StaticColumnIterator.columnsWithIDs();
            Field field = this.index.field("importfield");
            client.importField(field, iterator);
        }
    }

    @Test(expected = PilosaException.class)
    public void failUnknownErrorResponseTest() throws IOException {
        HttpServer server = runContent0HttpServer("/schema", 504);
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.readServerSchema();
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }

    @Test(expected = PilosaException.class)
    public void failNoCoordinatorTest() throws IOException {
        HttpServer server = runNoCoordinatorHttpServer();
        try (PilosaClient client = PilosaClient.withAddress(":15999")) {
            try {
                client.query(this.keyField.set(1, "foo"));
            } finally {
                if (server != null) {
                    server.stop(0);
                }
            }
        }
    }


    private static int counter = 0;

    private static String getRandomIndexName() {
        return String.format("testdb-%d", ++counter);
    }

    private HttpServer runImportFailsHttpServer() {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/internal/fragment/nodes", new FragmentNodesHandler());
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

    private HttpServer runNoCoordinatorHttpServer() {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/status", new NoCoordinatorHandler());
            server.setExecutor(null);
            server.start();
            return server;
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    private HttpServer runNonexistentCoordinatorHttpServer() {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/status", new NonexistentCoordinatorHandler());
            server.setExecutor(null);
            server.start();
            return server;
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    private HttpServer warningResponseHttpServer(String path, String warningMessage) {
        final int port = 15999;
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(path, new WarningResponseHandler(warningMessage));
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
            String response = "[{\"scheme\":\"http\", \"host\":\"localhost:15999\"}]";
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

    static class NoCoordinatorHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange r) throws IOException {
            String response = "{\"state\":\"NORMAL\",\"nodes\":[{\"id\":\"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc\",\"uri\":{\"scheme\":\"http\",\"host\":\"localhost\",\"port\":10101}}],\"localID\":\"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc\"}";
            r.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = r.getResponseBody()) {
                os.write(response.getBytes());
            }
            r.close();
        }
    }

    static class NonexistentCoordinatorHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange r) throws IOException {
            String response = "{\"state\":\"NORMAL\",\"nodes\":[{\"isCoordinator\":true, \"id\":\"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc\",\"uri\":{\"scheme\":\"http\",\"host\":\"nonexistent\",\"port\":4444}}],\"localID\":\"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc\"}";
            r.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = r.getResponseBody()) {
                os.write(response.getBytes());
            }
            r.close();
        }
    }

    static class WarningResponseHandler implements HttpHandler {
        WarningResponseHandler(String warningHeader) {
            super();
            this.warningHeader = warningHeader;
        }

        @Override
        public void handle(HttpExchange r) throws IOException {
            try (OutputStream os = r.getResponseBody()) {
                r.getResponseHeaders().set("warning", warningHeader);
                byte[] responseBytes = {0x12, 0x04, 0x0a, 0x00, 0x30, 0x01};
                r.sendResponseHeaders(200, responseBytes.length);
                os.write(responseBytes);
            }
            r.close();
        }

        private String warningHeader;
    }

    private Index findIndex(Schema schema, Index target) {
        for (Map.Entry<String, Index> entry : schema.getIndexes().entrySet()) {
            if (entry.getKey().equals(target.getName())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private Field findField(Schema schema, Field target) {
        Index index = findIndex(schema, target.getIndex());
        if (index != null) {
            for (Map.Entry<String, Field> entry : index.getFields().entrySet()) {
                if (entry.getKey().equals(target.getName())) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private PilosaClient getClient() {
        String bindAddress = getBindAddress();
        Cluster cluster = Cluster.withHost(URI.address(bindAddress));
        ClientOptions.Builder optionsBuilder = ClientOptions.builder();
        long shardWidth = getShardWidth();
        if (shardWidth > 0) {
            optionsBuilder.setShardWidth(shardWidth);
        }
        return new InsecurePilosaClientIT(cluster, optionsBuilder.build());
    }

    private String getBindAddress() {
        String bindAddress = System.getenv("PILOSA_BIND");
        if (bindAddress == null) {
            bindAddress = "http://:10101";
        }
        return bindAddress;
    }

    private boolean isLegacyModeOff() {
        String legacyModeOffStr = System.getenv("LEGACY_MODE_OFF");
        return legacyModeOffStr != null && legacyModeOffStr.equals("true");
    }

    private long getShardWidth() {
        String shardWidthStr = System.getenv("SHARD_WIDTH");
        return (shardWidthStr == null) ? 0 : Long.parseLong(shardWidthStr);
    }

    private RecordIterator csvRecordIterator(String path, LineDeserializer deserializer)
            throws FileNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL uri = loader.getResource(path);
        if (uri == null) {
            fail(String.format("%s not found", path));
        }
        return FileRecordIterator.fromPath(uri.getPath(), deserializer);
    }

}

class StaticColumnIterator implements RecordIterator {
    private List<Record> records;
    private int index = 0;

    public static StaticColumnIterator columnsWithIDs() {
        return new StaticColumnIterator(false, false);
    }

    public static StaticColumnIterator columnsWithKeys() {
        return new StaticColumnIterator(true, false);
    }

    public static StaticColumnIterator fieldValuesWithIDs() {
        return new StaticColumnIterator(false, true);
    }

    public static StaticColumnIterator fieldValuesWithKeys() {
        return new StaticColumnIterator(true, true);
    }

    private StaticColumnIterator(boolean keys, boolean intValues) {
        this.records = new ArrayList<>(3);
        if (keys) {
            if (intValues) {
                this.records.add(FieldValue.create("ten", 7));
                this.records.add(FieldValue.create("seven", 1));
            } else {
                this.records.add(Column.create("ten", "five"));
                this.records.add(Column.create("two", "three"));
                this.records.add(Column.create("seven", "one"));
            }
        } else {
            if (intValues) {
                this.records.add(FieldValue.create(10, 7));
                this.records.add(FieldValue.create(7, 1));
            } else {
                this.records.add(Column.create(10, 5));
                this.records.add(Column.create(2, 3));
                this.records.add(Column.create(7, 1));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return this.index < this.records.size();
    }

    @Override
    public Record next() {
        return this.records.get(index++);
    }

    @Override
    public void remove() {
        // We have this just to avoid compilation problems on JDK 7
    }
}

class StaticColumnIteratorWithTimestamp implements RecordIterator {
    private List<Record> records;
    private int index = 0;

    public static StaticColumnIteratorWithTimestamp columnsWithIDs() {
        return new StaticColumnIteratorWithTimestamp(false, false);
    }

    public static StaticColumnIteratorWithTimestamp columnsWithKeys() {
        return new StaticColumnIteratorWithTimestamp(true, false);
    }

    public static StaticColumnIteratorWithTimestamp fieldValuesWithIDs() {
        return new StaticColumnIteratorWithTimestamp(false, true);
    }

    public static StaticColumnIteratorWithTimestamp fieldValuesWithKeys() {
        return new StaticColumnIteratorWithTimestamp(true, true);
    }

    private StaticColumnIteratorWithTimestamp(boolean keys, boolean intValues) {
        this.records = new ArrayList<>(3);
        if (keys) {
            if (intValues) {
                this.records.add(FieldValue.create("ten", 7));
                this.records.add(FieldValue.create("seven", 1));
            } else {
                this.records.add(Column.create("ten", "five"));
                this.records.add(Column.create("two", "three"));
                this.records.add(Column.create("seven", "one"));

            }
        } else {
            if (intValues) {
                this.records.add(FieldValue.create(10, 7));
                this.records.add(FieldValue.create(7, 1));
            } else {
                this.records.add(Column.create(10, 5, 1542199376));
                this.records.add(Column.create(2, 3, 1520268300));
                this.records.add(Column.create(7, 1, 1330965900));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return this.index < this.records.size();
    }

    @Override
    public Record next() {
        return this.records.get(index++);
    }

    @Override
    public void remove() {
        // We have this just to avoid compilation problems on JDK 7
    }
}

class InvalidPilosaClient extends InsecurePilosaClientIT {
    InvalidPilosaClient(Cluster cluster, ClientOptions options) {
        super(cluster, options);
    }
}

class XColumnIterator implements RecordIterator {

    XColumnIterator(long maxID, long maxColumns) {
        this.maxID = maxID;
        this.maxColumns = maxColumns;
    }

    public boolean hasNext() {
        return this.maxColumns > 0;
    }

    public Column next() {
        this.maxColumns -= 1;
        long rowID = (long) (Math.random() * this.maxID);
        long columnID = (long) (Math.random() * this.maxID);
        return Column.create(rowID, columnID);
    }

    @Override
    public void remove() {

    }

    private long maxID;
    private long maxColumns;
}
