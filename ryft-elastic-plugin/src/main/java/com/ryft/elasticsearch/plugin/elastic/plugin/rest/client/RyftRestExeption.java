package com.ryft.elasticsearch.plugin.elastic.plugin.rest.client;

public class RyftRestExeption extends Exception {
    private static final long serialVersionUID = 2036944736575610220L;

    public RyftRestExeption(String failure) {
        super(failure);
    }

}
