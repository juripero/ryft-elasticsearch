package com.dataart.ryft.elastic.converter.ryftdsl;

public class RyftInputSpecifierRecord extends RyftInputSpecifier {

    private final String fieldName;
    private static final String INPUT_SPECIFIER = "RECORD";

    public RyftInputSpecifierRecord(String fieldName) {
        this.fieldName = fieldName;
    }

    public RyftInputSpecifierRecord() {
        this.fieldName = "";
    }

    @Override
    public String buildRyftString(Boolean isIndexedSearch) {
        StringBuilder result = new StringBuilder(INPUT_SPECIFIER);
        if (isIndexedSearch) {
            result.append(".doc");
        }
        if (!fieldName.isEmpty()) {
            result.append(".").append(fieldName);
        }
        return result.toString();
    }

}
