package com.pilosa.client;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Represent one of the results in the response.
 */
public final class QueryResult {
    private BitmapResult bitmapResult = null;
    private List<CountResultItem> countItems = null;
    private long count = 0L;

    QueryResult() {
    }

    QueryResult(BitmapResult bitmapResult, List<CountResultItem> countItems, long count) {
        this.bitmapResult = bitmapResult;
        this.countItems = countItems;
        this.count = count;
    }

    static QueryResult fromInternal(Internal.QueryResult q) {
        List<CountResultItem> items = new ArrayList<>(q.getPairsCount());
        for (Internal.Pair pair : q.getPairsList()) {
            items.add(CountResultItem.fromInternal(pair));
        }
        BitmapResult bitmapResult = null;
        if (q.hasBitmap()) {
            bitmapResult = BitmapResult.fromInternal(q.getBitmap());
        }
        return new QueryResult(bitmapResult, items, q.getN());
    }

    /**
     * Returns the bitmap result.
     *
     * @return bitmap result if it exists or <code>null</code>
     */
    public BitmapResult getBitmap() {
        return this.bitmapResult;
    }

    /**
     * Returns the count result items (from TopN query).
     *
     * @return count result items
     */
    public List<CountResultItem> getCountItems() {
        return this.countItems;
    }

    /**
     * Returns the result from Count query
     *
     * @return number of columns for the bitmap in the query
     */
    public long getCount() {
        return this.count;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof QueryResult)) {
            return false;
        }
        QueryResult rhs = (QueryResult) obj;
        return new EqualsBuilder()
                .append(bitmapResult, rhs.bitmapResult)
                .append(countItems, rhs.countItems)
                .append(count, rhs.count)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(bitmapResult)
                .append(countItems)
                .append(count)
                .toHashCode();
    }
}
