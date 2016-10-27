package com.dataart.ryft.elastic.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.dataart.ryft.disruptor.PostConstruct;

@Singleton
public class PropertiesProvider implements PostConstruct, Provider<RyftProperties> {
    private final ESLogger logger = Loggers.getLogger(getClass());

    public static final String PLUGIN_SETTINGS_INDEX ="ryft.plugin.settings.index";
    public static final String DISRUPTOR_CAPACITY = "ryft.disruptor.cpacity";
    public static final String WROKER_THREAD_COUNT = "ryft.rest.client.thread.num";
    public static final String HOST = "ryft.rest.client.host";
    public static final String PORT = "ryft.rest.client.port";
    public static final String RYFT_SEARCH_URL = "ryft.search.url";
    public static final String REQ_THREAD_NUM = "ryft.request.processing.thread.num";
    public static final String RESP_THREAD_NUM = "ryft.response.processing.thread.num";

    RyftProperties props;
    Map<String, Object> defaults = new HashMap<String, Object>();

    @Override
    public void onPostConstruct() {
        defaults.put(PLUGIN_SETTINGS_INDEX, "ryftpluginsettings");
        defaults.put(DISRUPTOR_CAPACITY, "1048576");
        defaults.put(WROKER_THREAD_COUNT, "2");
        defaults.put(HOST, "172.16.13.3");
        defaults.put(PORT, "8765");
        defaults.put(REQ_THREAD_NUM, "2");
        defaults.put(RESP_THREAD_NUM, "2");
        defaults.put(RYFT_SEARCH_URL, "http://172.16.13.3:8765");

        Properties defaultProps = new Properties();
        defaultProps.putAll(defaults);
        try {
            InputStream file = AccessController.doPrivileged((PrivilegedAction<InputStream>) () -> this.getClass()
                    .getClassLoader().getResourceAsStream("ryft.elastic.plugin.properties"));
            if (file != null) {
                Properties fileProps = new Properties();
                fileProps.load(file);
                defaultProps.putAll(fileProps);
                this.props = new RyftProperties(defaultProps);
            }

        } catch (IOException e) {
            logger.error("Failed to load properties", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public RyftProperties get() {
        return this.props;
    }

}
