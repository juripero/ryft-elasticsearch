package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl.RyftQuery;

public interface RyftRequestEventFactory {

    public RyftRequestEvent create(RyftQuery ryftQuery);

}
