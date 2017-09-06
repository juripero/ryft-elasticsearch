package com.ryft.elasticsearch.converter.ryftdsl;

public enum RyftFormat {
    JSON, XML, UTF8, RAW, UNKNOWN_FORMAT;

    public static RyftFormat get(String value) {
        try {
            return valueOf(value.toUpperCase());
        } catch (RuntimeException ex) {
            return UNKNOWN_FORMAT;
        }
    }
}
