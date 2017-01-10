package com.ryft.elasticsearch.plugin.processors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ryft.elasticsearch.plugin.disruptor.PostConstruct;
import com.ryft.elasticsearch.plugin.disruptor.messages.InternalEvent;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class RyftProcessor implements PostConstruct {
    ExecutorService executor;
    

    // TODO: [imasternoy] Configure thread pool via properties
    @Override
    public void onPostConstruct() {
        executor = Executors
                .newFixedThreadPool(getPoolSize(), new ThreadFactoryBuilder().setNameFormat(getName()).build());

    }

    public abstract void process(InternalEvent event);

    /**
     * Should return name for current pool impl
     *
     */
    public abstract String getName();
    
    public abstract int getPoolSize();

}
