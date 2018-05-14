# Data Model and Queries

## Indexes and Frames

*Index* and *frame*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`Schema.index` method is used to create an index object. Note that this does not create a index on the server; the index object simply defines the schema.

```java
Schema schema = Schema.defaultSchema();
Index repository = schema.index("repository");
```

Frames are created with a call to `Index.frame` method:

```java
Frame stargazer = repository.frame("stargazer");
```

You can pass custom options to frames:

```java
FrameOptions stargazerOptions = FrameOptions.builder()
    .setInverseEnabled(true)
    .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
    .build();

Frame stargazer = repository.frame("stargazer", stargazerOptions);
```

## Queries

Once you have indexes and frame objects created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows, with related methods attached to frames.

For instance, `Bitmap` queries work on rows; use a frame object to create those queries:

```java
PqlBitmapQuery bitmapQuery = stargazer.bitmap(1);  // corresponds to PQL: Bitmap(frame='stargazer', row=1)
```

`Union` queries work on columns; use the index object to create them:

```java
PqlQuery query = repository.union(bitmapQuery1, bitmapQuery2);
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.batchQuery` method is used for that purpose:

```java
PqlQuery query = repository.batchQuery(
    stargazer.bitmap(1),
    repository.union(stargazer.bitmap(100), stargazer.bitmap(5))
);
```

The recommended way of creating query objects is, using dedicated methods attached to index and frame objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use `index.rawQuery` method for that. Note that, query string is not validated before sending to the server:

```java
PqlQuery query = repository.rawQuery("Bitmap(frame='stargazer', row=5)");
```

This client supports [Range encoded fields](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use range encoded fields, a frame should be created with one or more integer fields. Each field should have their minimums and maximums set. Here's how you would do that using this library:
```java
Index index = schema.index("animals");
FrameOptions options = FrameOptions.builder()
        .addIntField("captivity", 0, 956)
        .build();
Frame frame = index.frame("traits", options);
client.syncSchema(schema);
PqlBatchQuery query = index.batchQuery();
long data[] = {3, 392, 47, 956, 219, 14, 47, 504, 21, 0, 123, 318};
for (int i = 0; i < data.length; i++) {
    long column = i + 1;
    query.add(frame.field("captivity").setValue(column, data[i]));
}
client.query(query);
```

Let's write a range query:
```java
// Query for all animals with more than 100 specimens
RangeField captivity = frame.field("captivity");
QueryResponse response = client.query(captivity.greaterThan(100));
System.out.println(response.getResult().getBitmap().getBits());

// Query for the total number of animals in captivity
response = client.query(captivity.sum());
System.out.println(response.getResult().getValue());
```

It's possible to pass a bitmap query to `sum`, so only columns where a row is set are filtered in:
```java
// Let's run a few setbit queries first
client.query(index.batchQuery(
        frame.setBit(42, 1),
        frame.setBit(42, 6)));
response = client.query(captivity.sum(frame.bitmap(42)));
System.out.println(response.getResult().getValue());
```

See the *Field* functions further below for the list of functions that can be used with a `RangeField`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `PqlQuery union(PqlBitmapQueries...)`
* `PqlQuery intersect(PqlBitmapQueries...)`
* `PqlQuery difference(PqlBitmapQueries...)`
* `PqlQuery xor(PqlBitmapQueries...)`
* `PqlQuery count(PqlBitmapQuery bitmap)`
* `PqlQuery setColumnAttrs(long id, Map<String, Object> attributes)`

Frame:

* `PqlBitmapQuery bitmap(long rowID)`
* `PqlQuery setBit(long rowID, long columnID)`
* `PqlQuery clearBit(long rowID, long columnID)`
* `PqlBitmapQuery topN(long n)`
* `PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap)`
* `PqlBitmapQuery topN(long n, PqlBitmapQuery bitmap, String field, Object... values)`
* `PqlBitmapQuery range(long rowID, Date start, Date end)`
* `PqlQuery setRowAttrs(long rowID, Map<String, Object> attributes)`
* `PqlBaseQuery sum(String field)`
* `PqlBaseQuery setFieldValue(long columnID, String field, long value)`
* (**deprecated**) `PqlBitmapQuery inverseBitmap(long columnID)`
* (**deprecated**) `PqlBitmapQuery inverseTopN(long n)`
* (**deprecated**) `PqlBitmapQuery inverseTopN(long n, PqlBitmapQuery bitmap)`
* (**deprecated**) `PqlBitmapQuery inverseTopN(long n, PqlBitmapQuery bitmap, String field, Object... values)`
* (**deprecated**) `PqlBitmapQuery inverseRange(long columnID, Date start, Date end)`

Field:

* `PqlBitmapQuery lessThan(long n)`
* `PqlBitmapQuery lessThanOrEqual(long n)`
* `PqlBitmapQuery greaterThan(long n)`
* `PqlBitmapQuery greaterThanOrEqual(long n)`
* `PqlBitmapQuery equals(long n)`
* `PqlBitmapQuery notEquals(long n)`
* `PqlBitmapQuery notNull()`
* `PqlBitmapQuery between(long a, long b)`
* `PqlBaseQuery sum()`
* `PqlBaseQuery sum(PqlBitmapQuery bitmap)`
* `PqlBaseQuery setValue(long columnID, long value)`
