package com.ryft.elasticsearch.plugin;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ryft.elasticsearch.converter.ElasticConverterRyft;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.ryft.elasticsearch.utils.PostConstruct;

@Singleton
public class PropertiesProvider implements PostConstruct, Provider<RyftProperties> {

    private static final ESLogger LOGGER = Loggers.getLogger(PropertiesProvider.class);
    // Global properties
    public static final String RYFT_INTEGRATION_ENABLED = "ryft_integration_enabled";
    public static final String SEARCH_QUERY_LIMIT = "ryft_query_limit";
    // Local
    public static final String PLUGIN_SETTINGS_INDEX = "ryft_plugin_settings_index";
    public static final String DISRUPTOR_CAPACITY = "ryft_disruptor_capacity";
    public static final String WORKER_THREAD_COUNT = "ryft_rest_client_thread_num";
    public static final String PORT = "ryft_rest_service_port";
    public static final String RYFT_REST_AUTH_ENABLED = "ryft_rest_auth_enabled";
    public static final String RYFT_REST_LOGIN = "ryft_rest_auth_login";
    public static final String RYFT_REST_PASSWORD = "ryft_rest_auth_password";
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
        defaults.put(SEARCH_QUERY_LIMIT, "1000");
        defaults.put(PLUGIN_SETTINGS_INDEX, "ryftpluginsettings");
        defaults.put(DISRUPTOR_CAPACITY, "1048576");
        defaults.put(WORKER_THREAD_COUNT, "2");
        defaults.put(PORT, "8765");
        defaults.put(REQ_THREAD_NUM, "2");
        defaults.put(RESP_THREAD_NUM, "2");
        defaults.put(RYFT_REST_AUTH_ENABLED, true);
        defaults.put(RYFT_REST_LOGIN, "admin");
        defaults.put(RYFT_REST_PASSWORD, "admin");
        defaults.put(RYFT_FORMAT, ElasticConverterRyft.ElasticConverterFormat.RyftFormat.JSON);
        defaults.put(RYFT_CASE_SENSITIVE, "false");

        props = new RyftProperties();
        props.putAll(defaults);
        try {
            FileInputStream file = AccessController.doPrivileged((PrivilegedAction<FileInputStream>) () -> {
                File jarPath = new File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
                String propertiesPath = jarPath.getParentFile().getAbsolutePath();
                LOGGER.info("propertiesPath: " + propertiesPath);
                try {
                    return new FileInputStream(propertiesPath + "/ryft.elastic.plugin.properties");
                } catch (Exception e) {
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
