package com.ryft.elasticsearch.converter.ryftdsl;

public interface RyftQuery extends RyftDslToken {

    public RyftQuery toRawTextQuery();
    public RyftQuery toLineQuery();

}
