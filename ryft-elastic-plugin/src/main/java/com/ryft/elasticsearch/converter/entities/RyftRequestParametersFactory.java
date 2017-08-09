package com.ryft.elasticsearch.converter.entities;

import com.ryft.elasticsearch.converter.ryftdsl.RyftQuery;
import java.util.Map;

public interface RyftRequestParametersFactory {
    
    public RyftRequestParameters create(RyftQuery ryftQuery, String[] indices, Map<String, Object> parsedQuery);

}
