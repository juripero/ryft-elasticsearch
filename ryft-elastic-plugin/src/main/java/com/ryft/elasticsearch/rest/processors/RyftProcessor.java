package com.ryft.elasticsearch.rest.processors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ryft.elasticsearch.utils.PostConstruct;
import com.ryft.elasticsearch.plugin.disruptor.messages.RequestEvent;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class RyftProcessor implements PostConstruct {

    protected ExecutorService executor ;

    @Override
    public void onPostConstruct() {
        executor = Executors
                .newFixedThreadPool(getPoolSize(), new ThreadFactoryBuilder().setNameFormat(getName()).build());
    }

    public abstract void process(RequestEvent event);

    /**
     * Should return name for current pool impl
     *
     * @return
     */
    public abstract String getName();

    public abstract int getPoolSize();

}
