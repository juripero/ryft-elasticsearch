/**
 * ##License
 * Ryft-Customized BSD License
 * Copyright (c) 2018, Ryft Systems, Inc.
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation and/or
 *   other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software must display the following acknowledgement:
 *   This product includes software developed by Ryft Systems, Inc.
 * 4. Neither the name of Ryft Systems, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY RYFT SYSTEMS, INC. ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL RYFT SYSTEMS, INC. BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
