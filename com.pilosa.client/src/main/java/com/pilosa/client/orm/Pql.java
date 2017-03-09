package com.pilosa.client.orm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.Validator;
import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.exceptions.ValidationException;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * ORM for PQL queries.
 * <p>
 * Sample usage:
 * <pre>
 * Client client = // ... create the client
 * QueryResponse = client.query("example_db",
 *                               Pql.union(Pql.bitmap(10, "foo"),
 *                                         Pql.bitmap(20, "foo")));
 * // process the response
 * </pre>
 *
 */
public final class Pql {
    Pql() {
    }

    /**
     * Creates a Bitmap query.
     *
     * @param id    bitmap ID
     * @param frame frame name
     * @return a PQL query
     * @throws ValidationException if an invalid frame name is passed
     */
    public static PqlBitmapQuery bitmap(long id, String frame) {
        Validator.ensureValidFrameName(frame);
        return new PqlBitmapQuery(String.format("Bitmap(id=%d, frame=\"%s\")", id, frame));
    }

    /**
     * Creates a SetBit query
     *
     * @param id        bitmap ID
     * @param frame     frame name
     * @param profileID profile ID
     * @throws ValidationException if an invalid frame name is passed
     * @return a PQL query
     */
    public static PqlQuery setBit(long id, String frame, long profileID) {
        Validator.ensureValidFrameName(frame);
        return new PqlQuery(String.format("SetBit(id=%d, frame=\"%s\", profileID=%d)", id, frame, profileID));
    }

    /**
     * Creates a ClearBit query
     *
     * @param id        bitmap ID
     * @param frame     frame name
     * @param profileID profile ID
     * @return a PQL query
     * @throws ValidationException if an invalid frame name is passed
     */
    public static PqlQuery clearBit(long id, String frame, long profileID) {
        Validator.ensureValidFrameName(frame);
        return new PqlQuery(String.format("ClearBit(id=%d, frame=\"%s\", profileID=%d)", id, frame, profileID));
    }

    /**
     * Creates a Union query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @param bitmaps other Bitmaps
     * @return a PQL query
     */
    public static PqlBitmapQuery union(PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Union", bitmap1, bitmap2, bitmaps);
    }

    /**
     * Creates an Intersect query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @param bitmaps other Bitmaps
     * @return a PQL query
     */
    public static PqlBitmapQuery intersect(PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Intersect", bitmap1, bitmap2, bitmaps);
    }

    /**
     * Creates a Difference query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @param bitmaps other Bitmaps
     * @return a PQL query
     */
    public static PqlBitmapQuery difference(PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        return bitmapOperation("Difference", bitmap1, bitmap2, bitmaps);
    }

    /**
     * Creates a Count query.
     *
     * @param bitmap the bitmap query
     * @return a PQL query
     */
    public static PqlQuery count(PqlBitmapQuery bitmap) {
        return new PqlQuery(String.format("Count(%s)", bitmap));
    }

    /**
     * Creates a TopN query.
     *
     * @param frame frame name
     * @param n     number of items to return
     * @return a PQL Bitmap query
     * @throws ValidationException if an invalid frame name is passed
     */
    public static PqlBitmapQuery topN(String frame, long n) {
        Validator.ensureValidFrameName(frame);
        return new PqlBitmapQuery(String.format("TopN(frame=\"%s\", n=%d)", frame, n));
    }

    /**
     * Creates a TopN query.
     *
     * @param bitmap the bitmap query
     * @param frame  frame name
     * @param n      number of items to return
     * @return a PQL query
     * @throws ValidationException if an invalid frame name is passed
     */
    public static PqlBitmapQuery topN(PqlBitmapQuery bitmap, String frame, long n) {
        Validator.ensureValidFrameName(frame);
        return new PqlBitmapQuery(String.format("TopN(%s, frame=\"%s\", n=%d)", bitmap, frame, n));
    }

    /**
     * Creates a TopN query.
     *
     * @param bitmap the bitmap query
     * @param frame  frame name
     * @param n      number of items to return
     * @param field  field name
     * @param values filter values to be matched against the field
     * @return a PQL query
     * @throws ValidationException if an invalid frame name is passed
     */
    public static PqlBitmapQuery topN(PqlBitmapQuery bitmap, String frame, long n, String field, Object... values) {
        Validator.ensureValidFrameName(frame);
        // TODO: validate field
        try {
            String valuesString = mapper.writeValueAsString(values);
            return new PqlBitmapQuery(String.format("TopN(%s, frame=\"%s\", n=%d, field=\"%s\", %s)",
                    bitmap, frame, n, field, valuesString));
        } catch (JsonProcessingException ex) {
            throw new PilosaException("Error while converting values", ex);
        }
    }

    /**
     * Creates a Range query.
     *
     * @param id    bitmap ID
     * @param frame frame name
     * @param start start timestamp
     * @param end   end timestamp
     * @return a PQL query
     * @throws ValidationException if an invalid frame name is passed
     */
    public static PqlQuery range(long id, String frame, Date start, Date end) {
        Validator.ensureValidFrameName(frame);
        DateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat fmtTime = new SimpleDateFormat("HH:mm");
        return new PqlQuery(String.format("Range(id=%d, frame=\"%s\", start=\"%sT%s\", end=\"%sT%s\")",
                id, frame, fmtDate.format(start), fmtTime.format(start), fmtDate.format(end), fmtTime.format(end)));
    }

    /**
     * Creates a SetBitmapAttrs query.
     *
     * @param id         bitmap ID
     * @param frame      frame name
     * @param attributes bitmap attributes
     * @return a PQL query
     * @throws ValidationException if an invalid frame name is passed
     */
    public static PqlQuery setBitmapAttrs(long id, String frame, Map<String, Object> attributes) {
        Validator.ensureValidFrameName(frame);
        String attributesString = createAttributesString(attributes);
        return new PqlQuery(String.format("SetBitmapAttrs(id=%d, frame=\"%s\", %s)",
                id, frame, attributesString));
    }

    /**
     * Creates a SetProfileAttrs query
     *
     * @param id         profile ID
     * @param attributes profile attributes
     * @return a PQL query
     */
    public static PqlQuery setProfileAttrs(long id, Map<String, Object> attributes) {
        String attributesString = createAttributesString(attributes);
        return new PqlQuery(String.format("SetProfileAttrs(id=%d, %s)",
                id, attributesString));
    }

    private static ObjectMapper mapper = new ObjectMapper();

    private static String createAttributesString(Map<String, Object> attributes) {
        // TODO: validate attributes
        try {
            List<String> kvs = new ArrayList<>(attributes.size());
            for (Map.Entry<String, Object> item : attributes.entrySet()) {
                kvs.add(String.format("%s=%s", item.getKey(), mapper.writeValueAsString(item.getValue())));
            }
            return StringUtils.join(kvs, ", ");
        } catch (JsonProcessingException ex) {
            throw new PilosaException("Error while converting values", ex);
        }
    }

    private static PqlBitmapQuery bitmapOperation(String name, PqlBitmapQuery bitmap1, PqlBitmapQuery bitmap2, PqlBitmapQuery... bitmaps) {
        String qry = String.format("%s, %s", bitmap1, bitmap2);
        if (bitmaps.length > 0) {
            StringBuilder builder = new StringBuilder(bitmaps.length);
            builder.append(qry);
            for (PqlBitmapQuery bitmap : bitmaps) {
                builder.append(", ");
                builder.append(bitmap);
            }
            qry = builder.toString();
        }
        return new PqlBitmapQuery(String.format("%s(%s)", name, qry));
    }
}
