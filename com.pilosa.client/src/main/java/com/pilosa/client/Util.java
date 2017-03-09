package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class Util {
    static final int PROTOBUF_STRING_TYPE = 1;
    static final int PROTOBUF_UINT_TYPE = 2;
    static final int PROTOBUF_BOOL_TYPE = 3;
    static final int PROTOBUF_DOUBLE_TYPE = 4;

    static Map<String, Object> protobufAttrsToMap(List<Internal.Attr> attrList) {
        Map<String, Object> attrs = new HashMap<>(attrList.size());
        for (Internal.Attr attr : attrList) {
            Object value;
            switch ((int) attr.getType()) {
                case PROTOBUF_STRING_TYPE:
                    value = attr.getStringValue();
                    break;
                case PROTOBUF_UINT_TYPE:
                    value = attr.getUintValue();
                    break;
                case PROTOBUF_BOOL_TYPE:
                    value = attr.getBoolValue();
                    break;
                case PROTOBUF_DOUBLE_TYPE:
                    value = attr.getFloatValue();
                    break;
                default:
                    throw new PilosaException("Unknown attribute field type: " + attr.getType());
            }
            attrs.put(attr.getKey(), value);
        }
        return attrs;
    }
}
