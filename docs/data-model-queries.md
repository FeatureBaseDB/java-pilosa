# Data Model and Queries

## Indexes and Fields

*Index* and *field*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`Schema.index` method is used to create an index object. Note that this does not create a index on the server; the index object simply defines the schema.

```java
Schema schema = Schema.defaultSchema();
Index repository = schema.index("repository");
```

Fields are created with a call to `Index.field` method:

```java
Field stargazer = repository.field("stargazer");
```

You can pass custom options to fields:

```java
FieldOptions stargazerOptions = FieldOptions.builder()
    .setInverseEnabled(true)
    .setTimeQuantum(TimeQuantum.YEAR_MONTH_DAY)
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

The recommended way of creating query objects is, using dedicated methods attached to index and field objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use `index.rawQuery` method for that. Note that, query string is not validated before sending to the server:

```java
PqlQuery query = repository.rawQuery("Row(field='stargazer', row=5)");
```

This client supports [Range encoded fields](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use range encoded fields, a field should be created with one or more integer fields. Each field should have their minimums and maximums set. Here's how you would do that using this library:
```java
Index index = schema.index("animals");
FieldOptions options = FieldOptions.builder()
        .addIntField("captivity", 0, 956)
        .build();
Field field = index.field("traits", options);
client.syncSchema(schema);
PqlBatchQuery query = index.batchQuery();
long data[] = {3, 392, 47, 956, 219, 14, 47, 504, 21, 0, 123, 318};
for (int i = 0; i < data.length; i++) {
    long column = i + 1;
    query.add(field.field("captivity").setValue(column, data[i]));
}
client.query(query);
```

Let's write a range query:
```java
// Query for all animals with more than 100 specimens
RangeField captivity = field.field("captivity");
QueryResponse response = client.query(captivity.greaterThan(100));
System.out.println(response.getResult().getRow().getBits());

// Query for the total number of animals in captivity
response = client.query(captivity.sum());
System.out.println(response.getResult().getValue());
```

It's possible to pass a row query to `sum`, so only columns where a row is set are filtered in:
```java
// Let's run a few setbit queries first
client.query(index.batchQuery(
        field.setBit(42, 1),
        field.setBit(42, 6)));
response = client.query(captivity.sum(field.row(42)));
System.out.println(response.getResult().getValue());
```

See the *Field* functions further below for the list of functions that can be used with a `RangeField`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `PqlQuery union(PqlRowQueries...)`
* `PqlQuery intersect(PqlRowQueries...)`
* `PqlQuery difference(PqlRowQueries...)`
* `PqlQuery xor(PqlRowQueries...)`
* `PqlQuery count(PqlRowQuery row)`
* `PqlQuery setColumnAttrs(long id, Map<String, Object> attributes)`

Field:

* `PqlRowQuery row(long rowID)`
* `PqlQuery setBit(long rowID, long columnID)`
* `PqlQuery clearBit(long rowID, long columnID)`
* `PqlRowQuery topN(long n)`
* `PqlRowQuery topN(long n, PqlRowQuery row)`
* `PqlRowQuery topN(long n, PqlRowQuery row, String field, Object... values)`
* `PqlRowQuery range(long rowID, Date start, Date end)`
* `PqlQuery setRowAttrs(long rowID, Map<String, Object> attributes)`
* `PqlBaseQuery sum(String field)`
* `PqlBaseQuery setFieldValue(long columnID, String field, long value)`
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
* `PqlBaseQuery setValue(long columnID, long value)`
