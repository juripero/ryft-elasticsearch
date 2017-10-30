package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RyftIndex {

    @JsonProperty("file")
    private String sourceFile;
    @JsonProperty("offset")
    private Long offset;
    @JsonProperty("length")
    private Long length;
    @JsonProperty("fuzziness")
    private Long fuzziness;
    @JsonProperty("host")
    private String host;

    public RyftIndex() {
        // TODO Auto-generated constructor stub
    }

    public RyftIndex(String sourceFile, Long offset, Long length, Long fuzziness, String host) {
        super();
        this.sourceFile = sourceFile;
        this.offset = offset;
        this.length = length;
        this.fuzziness = fuzziness;
        this.host = host;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Long getFuzziness() {
        return fuzziness;
    }

    public void setFuzziness(Long fuzziness) {
        this.fuzziness = fuzziness;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return "RyftIndex{" + "file=" + sourceFile + ", offset=" + offset + ", length=" + length + ", fuzziness=" + fuzziness + ", host=" + host + '}';
    }

}
