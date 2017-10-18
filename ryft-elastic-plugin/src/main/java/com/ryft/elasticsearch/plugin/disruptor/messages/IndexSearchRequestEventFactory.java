package com.ryft.elasticsearch.plugin.disruptor.messages;

import com.ryft.elasticsearch.converter.entities.RyftRequestParameters;

/**
 *
 * @author denis
 */
public interface IndexSearchRequestEventFactory {

    public IndexSearchRequestEvent create(RyftRequestParameters requestParameters);

}
