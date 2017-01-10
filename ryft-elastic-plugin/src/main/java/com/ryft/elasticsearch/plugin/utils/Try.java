package com.ryft.elasticsearch.plugin.utils;

/**
 * Boxing type for error handling like in Scala
 *
 * @param <T> Result
 */
public class Try<T> {

    public static <T> Try<T> apply(ThrowingSupplier<T> function) {
        try {
            T result = function.get();
            return new Try<>(result, null);
        } catch (Exception ex) {
            return new Try<>(null, ex);
        }
    }

    public static <T> Try<T> apply(Exception error) {
        return new Try<>(null, error);
    }

    public static <T> Try<T> apply(String errorDescription) {
        return new Try<>(null, new Exception(errorDescription));
    }

    private final T result;
    private final Exception error;

    private Try(T result, Exception error) {
        this.result = result;
        this.error = error;
    }

    public <R> Try<R> map(ThrowingFunction<T, R> function) {
        if (hasError()) {
            return new Try<>(null, error);
        } else {
            try {
                return new Try<>(function.apply(this.result), null);
            } catch (Exception ex) {
                return new Try<>(null, ex);
            }
        }
    }

    public void throwException() throws Exception {
        throw error;
    }

    public T getResultOrException() throws Exception {
        if (hasError()) {
            throwException();
        }
        return getResult();
    }

    public T getResult() {
        return result;
    }

    public Exception getError() {
        return error;
    }

    public Boolean hasError() {
        return error != null;
    }

    @Override
    public String toString() {
        return "Try{" + ((result == null) ? "" : String.format("result=%s", result))
                + ((error == null) ? "" : String.format("error=%s", error)) + '}';
    }

}
