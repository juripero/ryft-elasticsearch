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
package com.ryft.elasticsearch.plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class RyftProperties extends Properties {

    public RyftProperties() {
    }

    public RyftProperties(Map defaults) {
        this.putAll(defaults);
    }
    
    public Integer getInt(String key) {
        return (containsKey(key)) ? Integer.parseInt(getStr(key)) : null;
    }

    public String getStr(String key) {
        return (containsKey(key)) ? get(key).toString() : null;
    }

    public Boolean getBool(String key) {
        return (containsKey(key)) ? Boolean.parseBoolean(getStr(key)) : null;
    }

    public Long getLong(String key) {
        return (containsKey(key)) ? Long.parseLong(getStr(key)) : null;
    }

    public Double getDouble(String key) {
        return (containsKey(key)) ? Double.parseDouble(getStr(key)) : null;
    }

    public RyftProperties getRyftProperties(String key) {
        return (containsKey(key)) ? (RyftProperties) get(key) : null;
    }

    public <T> List<T> getList(String key, Class<T> clazz) {
        return (containsKey(key)) ? new ArrayList<>((Collection) get(key)) : null;
    }
    
    public Map<String, Object> toMap() {
        return entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue()));
    }
}
