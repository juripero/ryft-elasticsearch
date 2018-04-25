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
package com.ryft.elasticsearch.converter.ryftdsl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class RyftExpression implements RyftDslToken {

    protected final String expressionName;
    protected String value;

    protected Boolean line = null;
    protected Integer width = null;

    public RyftExpression(String expressionName) {
        this.expressionName = expressionName;
    }

    public RyftExpression(String expressionName, String value) {
        this(expressionName);
        this.value = value;
    }

    public RyftExpression(String expressionName, String value, Boolean line) {
        this(expressionName, value);
        this.line = line;
    }

    public RyftExpression(String expressionName, String value, Integer width) {
        this(expressionName, value);
        this.width = width;
    }

    protected List<String> getParameters() {
        List<String> result = new ArrayList<>();
        if (line != null) {
            result.add(String.format("LINE=%b", line));
        }
        if (width != null) {
            result.add(String.format("WIDTH=%d", width));
        }
        return result;
    }

    @Override
    public String buildRyftString() {
        String argument = Stream.concat(Stream.of(value), getParameters().stream()).collect(Collectors.joining(", "));
        if ((expressionName != null) && (!expressionName.isEmpty())) {
            return String.format("%s(%s)", expressionName, argument);
        } else {
            return value;
        }
    }

    public RyftExpression toLineExpression() {
        this.line = true;
        this.width = null;
        return this;
    }

    public RyftExpression toWidthExpression(Integer width) {
        this.width = width;
        return this;
    }
}
