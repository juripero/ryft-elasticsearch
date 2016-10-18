package com.dataart.ryft.elastic.parser.dsl;

public class RyftInputSpecifierRawText extends RyftInputSpecifier {

    private static final String INPUT_SPECIFIER = "RAW_TEXT";

    @Override
    public String buildRyftString() {
        return INPUT_SPECIFIER;
    }

}
