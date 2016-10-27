package com.dataart.ryft.utils;

@FunctionalInterface
public interface ThrowingSupplier<T> {

    T get() throws Exception;
}
