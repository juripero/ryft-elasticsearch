package com.ryft.elasticsearch.plugin.elastic.plugin;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;

/**
 * 
 * @author imasternoy
 */
public class RyftElasticPlugin extends Plugin {

    @Inject
    RestSearchActionFilter filter;

    public void onModule(ActionModule actionModule) {
        actionModule.registerFilter(RestSearchActionFilter.class);
    }

    @Override
    public Collection<Class<? extends Closeable>> indexServices() {
        return super.indexServices();
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.<Module> singletonList(new RyftElasticModule());
    }

    @Override
    public String name() {
        return "ryft-elastic-plugin";
    }

    @Override
    public String description() {
        return "Plugin responsible for routing edit distance searches to Ryft REST API";
    }

}
