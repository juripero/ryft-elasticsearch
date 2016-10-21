package com.dataart.ryft.elastic.converter;

import java.io.IOException;

public class ElasticConversionException extends IOException {

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
