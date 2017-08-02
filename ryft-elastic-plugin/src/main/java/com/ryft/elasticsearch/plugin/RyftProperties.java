package com.ryft.elasticsearch.plugin;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class RyftProperties extends Properties {

    public RyftProperties() {
    }

    public Integer getInt(String key) {
        return (containsKey(key)) ? Integer.parseInt(getStr(key)) : null;
    }

    public String getStr(String key) {
        return (containsKey(key)) ? get(key).toString() : null;
    }

    public Boolean getBool(String key) {
        return (containsKey(key)) ? Boolean.parseBoolean(getStr(key)) : null;
    }

    public Long getLong(String key) {
        return (containsKey(key)) ? Long.parseLong(getStr(key)) : null;
    }

    public RyftProperties getRyftProperties(String key) {
        return (containsKey(key)) ? (RyftProperties) get(key) : null;
    }

    public Map<String, Object> toMap() {
        return entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue()));
    }
}
