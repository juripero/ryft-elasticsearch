package com.dataart.ryft.disruptor.messages;

public class DisruptorEvent<T> {
    private T value;

    public T getEvent() {
        return value;
    }

    public void setEvent(T value) {
        this.value = value;
    }
}