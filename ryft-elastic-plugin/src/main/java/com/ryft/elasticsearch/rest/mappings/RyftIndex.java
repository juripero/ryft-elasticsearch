package com.ryft.elasticsearch.rest.mappings;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.elasticsearch.search.SearchShardTarget;

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

    public SearchShardTarget getSearchShardTarget() {
        try {
            Path sourcePath = Paths.get(sourceFile);
            String index = sourcePath.getName(4).toString();
            Integer shardId = -1;
            shardId = Integer.valueOf(sourcePath.getName(5).toString());
            return new SearchShardTarget(host, index, shardId);
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "RyftIndex{" + "file=" + sourceFile + ", offset=" + offset + ", length=" + length + ", fuzziness=" + fuzziness + ", host=" + host + '}';
    }

}
