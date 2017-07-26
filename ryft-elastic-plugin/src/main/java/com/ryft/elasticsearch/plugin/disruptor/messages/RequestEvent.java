package com.ryft.elasticsearch.plugin.disruptor.messages;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;

public abstract class RequestEvent {
    
    private ActionListener<SearchResponse> callback;

    public abstract EventType getEventType();

    public ActionListener<SearchResponse> getCallback() {
        return callback;
    }

    public void setCallback(ActionListener<SearchResponse> callback) {
        this.callback = callback;
    }
}
