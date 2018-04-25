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

import com.ryft.elasticsearch.converter.ryftdsl.RyftFormat;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.utils.PostConstruct;
import java.io.FileNotFoundException;

@Singleton
public class PropertiesProvider implements PostConstruct, Provider<RyftProperties> {

    private static final ESLogger LOGGER = Loggers.getLogger(PropertiesProvider.class);
    // Global properties
    public static final String RYFT_INTEGRATION_ENABLED = "ryft_integration_enabled";
    // Local
    public static final String PLUGIN_SETTINGS_INDEX = "ryft_plugin_settings_index";
    public static final String DISRUPTOR_CAPACITY = "ryft_disruptor_capacity";
    public static final String RESPONSE_BUFFER_SIZE = "ryft_response_buffer_size";
    public static final String WORKER_THREAD_COUNT = "ryft_rest_client_thread_num";
    public static final String PORT = "ryft_rest_service_port";
    public static final String RYFT_REST_AUTH_ENABLED = "ryft_rest_auth_enabled";
    public static final String RYFT_REST_LOGIN = "ryft_rest_auth_login";
    public static final String RYFT_REST_PASSWORD = "ryft_rest_auth_password";
    public static final String REQ_THREAD_NUM = "ryft_request_processing_thread_num";
    public static final String RESP_THREAD_NUM = "ryft_response_processing_thread_num";
    public static final String AGGREGATIONS_ON_RYFT_SERVER = "ryft_aggregations_on_ryft_server";
    // Query properties
    public static final String RYFT_FILES_TO_SEARCH = "ryft_files";
    public static final String RYFT_FORMAT = "ryft_format";
    public static final String RYFT_CASE_SENSITIVE = "ryft_case_sensitive";
    public static final String RYFT_MAPPING = "ryft_mapping";
    public static final String ES_RESULT_SIZE = "es_result_size";

    private RyftProperties props;
    private final Map<String, Object> defaults = new HashMap<>();

    @Override
    public void onPostConstruct() {
        defaults.put(RYFT_INTEGRATION_ENABLED, "false");
        defaults.put(ES_RESULT_SIZE, "1000");
        defaults.put(PLUGIN_SETTINGS_INDEX, "ryftpluginsettings");
        defaults.put(DISRUPTOR_CAPACITY, "1048576");
        defaults.put(RESPONSE_BUFFER_SIZE, "104857600");
        defaults.put(WORKER_THREAD_COUNT, "2");
        defaults.put(PORT, "8765");
        defaults.put(REQ_THREAD_NUM, "2");
        defaults.put(RESP_THREAD_NUM, "2");
        defaults.put(RYFT_REST_AUTH_ENABLED, true);
        defaults.put(RYFT_REST_LOGIN, "admin");
        defaults.put(RYFT_REST_PASSWORD, "admin");
        defaults.put(RYFT_FORMAT, RyftFormat.JSON);
        defaults.put(RYFT_CASE_SENSITIVE, "false");
        defaults.put(AGGREGATIONS_ON_RYFT_SERVER, "");

        props = new RyftProperties();
        props.putAll(defaults);
        try {
            FileInputStream file = AccessController.doPrivileged((PrivilegedAction<FileInputStream>) () -> {
                File jarPath = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
                String propertiesPath = jarPath.getParentFile().getAbsolutePath();
                LOGGER.info("propertiesPath: " + propertiesPath);
                try {
                    return new FileInputStream(propertiesPath + "/ryft.elastic.plugin.properties");
                } catch (FileNotFoundException e) {
                    LOGGER.error("Failed to load properties", e);
                    return null;
                }
            });
            if (file != null) {
                Properties fileProps = new Properties();
                fileProps.load(file);
                props.putAll(fileProps);
            } else {
                LOGGER.warn("Failed to read properties from file");
            }
        } catch (IOException e) {
            LOGGER.error("Failed to load properties", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public RyftProperties get() {
        return props;
    }

}
