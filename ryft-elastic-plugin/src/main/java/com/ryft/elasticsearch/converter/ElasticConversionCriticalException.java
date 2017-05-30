package com.ryft.elasticsearch.converter;

public class ElasticConversionCriticalException extends ElasticConversionException {

    public ElasticConversionCriticalException() {
    }

    public ElasticConversionCriticalException(String message) {
        super(message);
    }

    public ElasticConversionCriticalException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticConversionCriticalException(Throwable cause) {
        super(cause);
    }
}
