package com.ryft.elasticsearch.converter.entities;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;

public interface RyftRequestParametersFactory {
    
    public RyftRequestParameters create(RyftQuery ryftQuery, String[] indices, ObjectNode parsedQuery);

}
