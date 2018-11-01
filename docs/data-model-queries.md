# Data Model and Queries

## Indexes and Fields

*Index* and *field*s are the main data models of Pilosa. You can check [Pilosa documentation](https://www.pilosa.com/docs) for more details about the data model.

`Schema.index` method is used to create an index object. Note that this does not create a index on the server; the index object simply defines the schema.

```java
Schema schema = Schema.defaultSchema();
Index repository = schema.index("repository");
```

You can pass custom options to indexes:
```java
IndexOptions indexOptions = IndexOptions.builder()
    .setKeys(true)
    .build();
Index repository = schema.index("repository", indexOptions);
```

Fields are created with a call to `Index.field` method:

```java
Field stargazer = repository.field("stargazer");
```

You can pass custom options to fields:

```java
FieldOptions stargazerOptions = FieldOptions.builder()
    .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
    .setKeys(true)
    .build();

Field stargazer = repository.field("stargazer", stargazerOptions);
```

## Queries

Once you have indexes and field objects created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows, with related methods attached to fields.

For instance, `Row` queries work on rows; use a field object to create those queries:

```java
PqlRowQuery rowQuery = stargazer.row(1);  // corresponds to PQL: Row(field='stargazer', row=1)
```

`Union` queries work on columns; use the index object to create them:

```java
PqlQuery query = repository.union(rowQuery1, rowQuery2);
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.batchQuery` method is used for that purpose:

```java
PqlQuery query = repository.batchQuery(
    stargazer.row(1),
    repository.union(stargazer.row(100), stargazer.row(5))
);
```

Note that the recommended way of posting large amounts of data is importing them instead of many `Set` or `Clear` queries. See [imports] to learn more.

The recommended way of creating query objects is, using dedicated methods attached to index and field objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use `index.rawQuery` method for that. The recommended way of creating query objects is, using dedicated methods attached to index and field objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use the `index.raw_query` method for that. Note that, the query string is not validated before sending to the server. Also, raw queries may be less efficient than the corresponding ORM query, since they are only sent to the coordinator node.

```java
PqlQuery query = repository.rawQuery("Row(stargazer=5)");
```

This client supports [Range encoded fields](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use range encoded fields, an `int` field should be created with minimum and maxium values defined beforehand. Here's how you would do that using this library:

```java
Index index = schema.index("animals");
FieldOptions options = FieldOptions.builder()
        // minimum value that this field can contain is 0, and the maximum is 956
        .fieldInt(0, 956)
        .build();
Field captivity = index.field("captivity", options);
// Materialize the schema on the server
client.syncSchema(schema);
long data[] = {3, 392, 47, 956, 219, 14, 47, 504, 21, 0, 123, 318};
PqlBatchQuery query = index.batchQuery(data.length);
for (int i = 0; i < data.length; i++) {
    long columnID = i + 1;
    query.add(captivity.setValue(columnID, data[i]));
}
client.query(query);
```

Let's write a range query:
```java
// Query for all animals with more than 100 specimens
QueryResponse response = client.query(captivity.greaterThan(100));
System.out.println(response.getResult().getRow().getColumns());

// Query for the total number of animals in captivity
response = client.query(captivity.sum());
System.out.println(response.getResult().getValue());
```

It's possible to pass a row query to `sum`, so only columns where a row is set are filtered in:
```java
// Let's run a few set queries first
client.query(index.batchQuery(
        field.set(42, 3),
        field.set(42, 392)));
response = client.query(captivity.sum(field.row(42)));
System.out.println(response.getResult().getValue());
```

See the *Field* functions further below for the list of functions that can be used with a `RangeField`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `PqlQuery union(rowQueries)`
* `PqlQuery intersect(rowQueries)`
* `PqlQuery difference(rowQueries)`
* `PqlQuery xor(rowQueries)`
* `PqlQuery nor(rowQuery)`
* `PqlQuery count(rowQuery)`
* `PqlQuery setColumnAttrs(columnID/columnKey, Map<String, Object> attributes)`
* `PqlBaseQuery options(rowQuery, OptionsOptions opts)`

Field:

* `PqlRowQuery row(rowID/rowKey/rowBool)`
* `PqlQuery set(rowID/rowKey/rowBool, columnID/columnKey)`
* `PqlQuery clear(rowID/rowKey/rowBool, columnID/columnKey)`
* `PqlRowQuery topN(long n)`
* `PqlRowQuery topN(long n, PqlRowQuery row)`
* `PqlRowQuery topN(long n, PqlRowQuery row, String field, Object... values)`
* `PqlRowQuery range(columnID/columnKey, Date start, Date end)`
* `PqlQuery setRowAttrs(rowID/rowKey/rowBool, Map<String, Object> attributes)`
* `PqlBaseQuery store(rowQuery, rowID/rowKey/rowBool)`
* `PqlBaseQuery clearRow(rowID/rowKey/rowBool)`
* `PqlBaseQuery sum(String field)`
* `PqlRowQuery lessThan(long n)`
* `PqlRowQuery lessThanOrEqual(long n)`
* `PqlRowQuery greaterThan(long n)`
* `PqlRowQuery greaterThanOrEqual(long n)`
* `PqlRowQuery equals(long n)`
* `PqlRowQuery notEquals(long n)`
* `PqlRowQuery notNull()`
* `PqlRowQuery between(long a, long b)`
* `PqlBaseQuery sum()`
* `PqlBaseQuery sum(PqlRowQuery row)`
* `PqlBaseQuery setValue(columnID/columnKey, long value)`
