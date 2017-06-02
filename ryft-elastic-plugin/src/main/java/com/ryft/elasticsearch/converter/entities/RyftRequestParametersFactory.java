package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;

public interface RyftRequestParametersFactory {
    
    public RyftRequestParameters create(RyftQuery ryftQuery, String[] indices, String agg);

}
