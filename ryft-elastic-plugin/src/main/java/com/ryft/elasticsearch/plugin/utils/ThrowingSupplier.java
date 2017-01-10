package com.ryft.elasticsearch.plugin.utils;

@FunctionalInterface
public interface ThrowingSupplier<T> {

    T get() throws Exception;
}
