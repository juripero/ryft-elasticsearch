package com.ryft.elasticsearch.plugin.elastic.converter;

public class ElasticConversionException extends Exception {

    public ElasticConversionException() {
    }

    public ElasticConversionException(String message) {
        super(message);
    }

    public ElasticConversionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticConversionException(Throwable cause) {
        super(cause);
    }

}
