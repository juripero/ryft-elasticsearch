package com.ryft.elasticsearch.rest.client;

public class RyftSearchException extends Exception {
    private static final long serialVersionUID = 2036944736575610220L;

    public RyftSearchException() {
    }

    public RyftSearchException(String message) {
        super(message);
    }

    public RyftSearchException(String message, Throwable cause) {
        super(message, cause);
    }

    public RyftSearchException(Throwable cause) {
        super(cause);
    }

}
