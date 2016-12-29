package com.dataart.ryft.elastic.converter.ryftdsl;

public interface RyftQuery extends RyftDslToken {

    public RyftQuery toRawTextQuery();

}
