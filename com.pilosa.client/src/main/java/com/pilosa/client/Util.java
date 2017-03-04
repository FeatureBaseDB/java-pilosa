package com.pilosa.client;

import com.pilosa.client.exceptions.PilosaException;
import com.pilosa.client.internal.ClientProtos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class Util {
    static Map<String, Object> protobufAttrsToMap(List<ClientProtos.Attr> attrList) {
        Map<String, Object> attrs = new HashMap<>(attrList.size());
        for (ClientProtos.Attr attr : attrList) {
            Object value;
            switch ((int) attr.getType()) {
                case ClientProtos.Attr.BOOLVALUE_FIELD_NUMBER:
                    value = attr.getBoolValue();
                    break;
                case ClientProtos.Attr.UINTVALUE_FIELD_NUMBER:
                    value = attr.getUintValue();
                    break;
                case ClientProtos.Attr.STRINGVALUE_FIELD_NUMBER:
                case ClientProtos.Attr.KEY_FIELD_NUMBER:  // XXX:
                    value = attr.getStringValue();
                    break;
                default:
                    throw new PilosaException("Unknown attribute field type: " + attr.getType());
            }
            attrs.put(attr.getKey(), value);
        }
        return attrs;
    }
}
