package com.dataart.ryft.elastic.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.dataart.ryft.disruptor.PostConstruct;

@Singleton
public class PropertiesProvider implements PostConstruct, Provider<RyftProperties> {
    private final ESLogger logger = Loggers.getLogger(getClass());

    public static final String DISRUPTOR_CAPACITY = "ryft.disruptor.cpacity";
    public static final String WROKER_THREAD_COUNT = "ryft.rest.client.thread.num";
    public static final String HOST = "ryft.rest.client.host";
    public static final String PORT = "ryft.rest.client.port";
    public static final String RYFT_SEARCH_URL = "ryft.search.url";
    public static final String REQ_THREAD_NUM = "ryft.request.processing.thread.num";
    public static final String RESP_THREAD_NUM = "ryft.response.processing.thread.num";

    RyftProperties  props;

    @Override
    public void onPostConstruct() {
        Properties props = new Properties();
        try {
            InputStream file = ClassLoader.getSystemResourceAsStream("ryft.elastic.plugin.properties");
            props.load(file);
            this.props = new RyftProperties(props);
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
