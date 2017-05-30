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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fuzziness == null) ? 0 : fuzziness.hashCode());
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + ((length == null) ? 0 : length.hashCode());
        result = prime * result + ((offset == null) ? 0 : offset.hashCode());
        result = prime * result + ((sourceFile == null) ? 0 : sourceFile.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RyftIndex other = (RyftIndex) obj;
        if (fuzziness == null) {
            if (other.fuzziness != null)
                return false;
        } else if (!fuzziness.equals(other.fuzziness))
            return false;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (length == null) {
            if (other.length != null)
                return false;
        } else if (!length.equals(other.length))
            return false;
        if (offset == null) {
            if (other.offset != null)
                return false;
        } else if (!offset.equals(other.offset))
            return false;
        if (sourceFile == null) {
            if (other.sourceFile != null)
                return false;
        } else if (!sourceFile.equals(other.sourceFile))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RyftIndex [sourceFile=" + sourceFile + ", offset=" + offset + ", length=" + length + ", fuzziness="
                + fuzziness + ", host=" + host + "]";
    }

}
