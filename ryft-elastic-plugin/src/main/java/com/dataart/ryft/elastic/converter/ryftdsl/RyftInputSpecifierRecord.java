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
    public String buildRyftString() {
        if (fieldName.isEmpty()) {
            return INPUT_SPECIFIER;
        } else {
            return String.format("%s.doc.%s", INPUT_SPECIFIER, fieldName);
        }
    }

}
