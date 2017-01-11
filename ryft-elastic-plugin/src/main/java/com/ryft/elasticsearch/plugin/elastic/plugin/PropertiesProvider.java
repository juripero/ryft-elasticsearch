package com.ryft.elasticsearch.plugin.elastic.plugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ryft.elasticsearch.plugin.elastic.converter.ElasticConverterRyft;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.plugin.disruptor.PostConstruct;

@Singleton
public class PropertiesProvider implements PostConstruct, Provider<RyftProperties> {

    private static final ESLogger LOGGER = Loggers.getLogger(PropertiesProvider.class);
    // Global properties
    public static final String RYFT_INTEGRATION_ENABLED = "ryft_integration_enabled";
    public static final String SEARCH_QUERY_SIZE = "ryft_query_limit";
    // Local
    public static final String PLUGIN_SETTINGS_INDEX = "ryft_plugin_settings_index";
    public static final String DISRUPTOR_CAPACITY = "ryft_disruptor_capacity";
    public static final String WROKER_THREAD_COUNT = "ryft_rest_client_thread_num";
    public static final String HOST = "ryft_rest_client_host";
    public static final String PORT = "ryft_rest_client_port";
    public static final String RYFT_REST_AUTH = "ryft_rest_auth";
    public static final String REQ_THREAD_NUM = "ryft_request_processing_thread_num";
    public static final String RESP_THREAD_NUM = "ryft_response_processing_thread_num";
    // Query properties
    public static final String RYFT_FILES_TO_SEARCH = "ryft_files";
    public static final String RYFT_FORMAT = "ryft_format";
    public static final String RYFT_CASE_SENSITIVE = "ryft_case_sensitive";

    private RyftProperties props;
    private final Map<String, Object> defaults = new HashMap<>();

    @Override
    public void onPostConstruct() {
        defaults.put(RYFT_INTEGRATION_ENABLED, "false");
        defaults.put(SEARCH_QUERY_SIZE, "1000");
        defaults.put(PLUGIN_SETTINGS_INDEX, "ryftpluginsettings");
        defaults.put(DISRUPTOR_CAPACITY, "1048576");
        defaults.put(WROKER_THREAD_COUNT, "2");
        defaults.put(HOST, "172.16.13.3");
        defaults.put(PORT, "8765");
        defaults.put(REQ_THREAD_NUM, "2");
        defaults.put(RESP_THREAD_NUM, "2");
        defaults.put(RYFT_REST_AUTH, "YWRtaW46YWRt  aW4=");
        defaults.put(RYFT_FORMAT, ElasticConverterRyft.ElasticConverterFormat.RyftFormat.JSON);
        defaults.put(RYFT_CASE_SENSITIVE, "false");

        props = new RyftProperties();
        props.putAll(defaults);
        try {
            FileInputStream file = AccessController.doPrivileged((PrivilegedAction<FileInputStream>) () -> {
                File jarPath = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
                String propertiesPath = jarPath.getParentFile().getAbsolutePath();
                System.out.println(" propertiesPath-" + propertiesPath);
                try {
                    return new FileInputStream(propertiesPath + "/ryft.elastic.plugin.properties");
                } catch (Exception e) {
                    LOGGER.error("Failed to load properties");
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
