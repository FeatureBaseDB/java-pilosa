package com.pilosa.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * ORM for PQL queries.
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
     */
    public static String bitmap(int id, String frame) {
        return String.format("Bitmap(id=%d, frame=\"%s\")", id, frame);
    }

    /**
     * Creates a SetBit query
     *
     * @param id        bitmap ID
     * @param frame     frame name
     * @param profileID profile ID
     * @return a PQL query
     */
    public static String setBit(int id, String frame, int profileID) {
        return String.format("SetBit(id=%d, frame=\"%s\", profileID=%d)", id, frame, profileID);
    }

    /**
     * Creates a ClearBit query
     *
     * @param id        bitmap ID
     * @param frame     frame name
     * @param profileID profile ID
     * @return a PQL query
     */
    public static String clearBit(int id, String frame, int profileID) {
        return String.format("ClearBit(id=%d, frame=\"%s\", profileID=%d)", id, frame, profileID);
    }

    /**
     * Creates a Union query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @return a PQL query
     */
    public static String union(String bitmap1, String bitmap2) {
        return String.format("Union(%s, %s)", bitmap1, bitmap2);
    }

    /**
     * Creates an Intersect query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @return a PQL query
     */
    public static String intersect(String bitmap1, String bitmap2) {
        return String.format("Intersect(%s, %s)", bitmap1, bitmap2);
    }

    /**
     * Creates a Difference query.
     *
     * @param bitmap1 first Bitmap
     * @param bitmap2 second Bitmap
     * @return a PQL query
     */
    public static String difference(String bitmap1, String bitmap2) {
        return String.format("Difference(%s, %s)", bitmap1, bitmap2);
    }

    /**
     * Creates a Count query.
     *
     * @param bitmap the bitmap query
     * @return a PQL query
     */
    public static String count(String bitmap) {
        return String.format("Count(%s)", bitmap);
    }

    /**
     * Creates a TopN query.
     *
     * @param bitmap the bitmap query
     * @param frame  frame name
     * @param n      number of items to return
     * @return a PQL query
     */
    public static String topN(String bitmap, String frame, int n) {
        return String.format("TopN(%s, frame=\"%s\", n=%d)", bitmap, frame, n);
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
     */
    public static String topN(String bitmap, String frame, int n, String field, Object... values) {
        try {
            String valuesString = mapper.writeValueAsString(values);
            return String.format("TopN(%s, frame=\"%s\", n=%d, field=\"%s\", %s)",
                    bitmap, frame, n, field, valuesString);
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
     */
    public static String range(int id, String frame, Date start, Date end) {
        DateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat fmtTime = new SimpleDateFormat("HH:mm");
        return String.format("Range(id=%d, frame=\"%s\", start=\"%sT%s\", end=\"%sT%s\")",
                id, frame, fmtDate.format(start), fmtTime.format(start), fmtDate.format(end), fmtTime.format(end));
    }

    /**
     * Creates a SetBitmapAttrs query.
     *
     * @param id         bitmap ID
     * @param frame      frame name
     * @param attributes bitmap attributes
     * @return a PQL query
     */
    public static String setBitmapAttrs(int id, String frame, Map<String, Object> attributes) {
        String attributesString = createAttributesString(attributes);
        return String.format("SetBitmapAttrs(id=%d, frame=\"%s\", %s)",
                id, frame, attributesString);
    }

    /**
     * Creates a SetProfileAttrs query
     *
     * @param id         profile ID
     * @param attributes profile attributes
     * @return a PQL query
     */
    public static String setProfileAttrs(int id, Map<String, Object> attributes) {
        String attributesString = createAttributesString(attributes);
        return String.format("SetProfileAttrs(id=%d, %s)",
                id, attributesString);
    }

    private static ObjectMapper mapper = new ObjectMapper();

    private static String createAttributesString(Map<String, Object> attributes) {
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
}
