package com.dataart.ryft.processors;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.TypeLiteral;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dataart.ryft.disruptor.RyftRequestEventConsumer;
import com.dataart.ryft.disruptor.messages.DisruptorEvent;
import com.dataart.ryft.disruptor.messages.EventType;
import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionExactSearch;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftInputSpecifierRecord;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuerySimple;
import com.dataart.ryft.elastic.plugin.JSR250Module;
import com.dataart.ryft.elastic.plugin.PropertiesProvider;
import com.dataart.ryft.elastic.plugin.RyftProperties;
import com.dataart.ryft.elastic.plugin.rest.client.RyftRestClient;

public class RyftRequestProcessorTest {

    private Injector injector;

    private static final String SEARCH_FIELD = "text_entry";
    private static final RyftQuery RYFT_QUERY
            = new RyftQuerySimple(
                    new RyftInputSpecifierRecord(SEARCH_FIELD), RyftOperator.CONTAINS,
                    new RyftExpressionExactSearch("To be, or not to be"));

    @Before
    public void init() {
        injector = Guice.createInjector(new TestModule(), new ProcessorsModule());
    }

    // TODO: [imasternoy] Currently ignored would need changes after adding ES
    // syntax support
    @Test
    @Ignore
    public void requestProcessorTest() throws Exception {
        RyftRestClient client = mock(RyftRestClient.class);
        RyftRequestProcessor processor = new RyftRequestProcessor(injector.getInstance(RyftProperties.class), client);
        ExecutorService executor = mock(ExecutorService.class);
        Future mockFuture = mock(Future.class);
        when(executor.submit((Runnable) any(Runnable.class))).thenReturn(mockFuture);
        processor.executor = executor;
        Map processors = injector.getInstance(Key.get(new TypeLiteral<Map<EventType, RyftProcessor>>() {
        }));

        Map<EventType, RyftProcessor> mockedProcessors = mock(Map.class);
        when(mockedProcessors.get(EventType.ES_REQUEST)).thenReturn(processor);
        RyftRequestEventConsumer consumer = new RyftRequestEventConsumer(mockedProcessors);

        DisruptorEvent event = new DisruptorEvent<RyftRequestEvent>();
        event.setEvent(new RyftRequestEvent(RYFT_QUERY));

        consumer.onEvent(event, 1235L, true);
        verify(mockedProcessors, times(1)).get(EventType.ES_REQUEST);
        verify(executor, times(1)).submit(any(Runnable.class));
        processor.executor.shutdown();
    }

    // TODO: [imasternoy] Currently ignored would need changes after adding ES
    // syntax support
    @Test
    @Ignore
    public void processorProcess() {
        ChannelFuture chFuture = mock(ChannelFuture.class);
        RyftRestClient client = mock(RyftRestClient.class);
        Channel channel = mock(Channel.class);
        when(channel.writeAndFlush(any())).thenReturn(chFuture);
        when(client.get()).thenReturn(channel);
        ChannelPipeline pipeline = mock(ChannelPipeline.class);
        when(channel.pipeline()).thenReturn(pipeline);
        RyftRequestProcessor processor = new RyftRequestProcessor(injector.getInstance(RyftProperties.class), client);
        RyftRequestEvent event = new RyftRequestEvent(RYFT_QUERY);
        event.setIndex((String[]) Arrays.asList("shakspeare").toArray());
        processor.sendToRyft(event);
        verify(client, times(1)).get();
        verify(channel, times(1)).writeAndFlush(any());

    }

    class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            install(new JSR250Module());
            bind(RyftProperties.class).toProvider(PropertiesProvider.class);
        }
    }
}
