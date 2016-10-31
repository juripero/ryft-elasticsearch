package com.dataart.ryft.elastic.plugin;

import java.util.Properties;

public class RyftProperties {
    Properties properties;

    public RyftProperties(Properties props) {
        this.properties = props;
    }

    public Integer getInt(String key) {
        return Integer.parseInt((String) properties.get(key));
    }

    public String getStr(String key) {
        return (String) properties.get(key);
    }
    
    public Boolean getBool(String key) {
        return Boolean.parseBoolean((String) properties.get(key));
    }

}
