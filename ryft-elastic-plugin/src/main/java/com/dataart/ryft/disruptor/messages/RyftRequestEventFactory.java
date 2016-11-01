package com.dataart.ryft.disruptor.messages;

import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;

public interface RyftRequestEventFactory {

    public RyftRequestEvent create(RyftQuery ryftQuery);

}
