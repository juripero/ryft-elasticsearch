package com.ryft.elasticsearch.plugin;

import java.util.Properties;

public class RyftProperties extends Properties {

    public RyftProperties() {
    }

    public Integer getInt(String key) {
        return Integer.parseInt(getStr(key));
    }

    public String getStr(String key) {
        return get(key).toString();
    }

    public Boolean getBool(String key) {
        return Boolean.parseBoolean(getStr(key));
    }

}
