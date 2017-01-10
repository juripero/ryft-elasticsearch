package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

public interface RyftQuery extends RyftDslToken {

    public RyftQuery toRawTextQuery();
    public RyftQuery toLineQuery();

}
