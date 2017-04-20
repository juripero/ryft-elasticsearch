package com.ryft.elasticsearch.plugin.elastic.converter.ryftdsl;

public class RyftInputSpecifierRawText extends RyftInputSpecifier {

    private static final String INPUT_SPECIFIER = "RAW_TEXT";

    @Override
    public String buildRyftString() {
        return INPUT_SPECIFIER;
    }

}
