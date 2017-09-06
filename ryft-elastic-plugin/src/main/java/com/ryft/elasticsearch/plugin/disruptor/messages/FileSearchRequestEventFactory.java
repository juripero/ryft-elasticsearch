package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;

public interface FileSearchRequestEventFactory {

    public FileSearchRequestEvent create(RyftRequestParameters requestParameters);

}
