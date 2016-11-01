package com.dataart.ryft.elastic.plugin.rest.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.junit.Before;
import org.junit.Test;

import com.dataart.ryft.disruptor.messages.RyftRequestEvent;
import com.dataart.ryft.disruptor.messages.RyftRequestEventFactory;
import com.dataart.ryft.elastic.converter.ElasticConversionModule;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftExpressionExactSearch;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftInputSpecifierRecord;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftOperator;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuery;
import com.dataart.ryft.elastic.converter.ryftdsl.RyftQuerySimple;
import com.dataart.ryft.elastic.plugin.JSR250Module;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;

public class RestClientHandlerTest {

    private static final String SEARCH_FIELD = "text_entry";
    private static final RyftQuery RYFT_QUERY
            = new RyftQuerySimple(
                    new RyftInputSpecifierRecord(SEARCH_FIELD), RyftOperator.CONTAINS,
                    new RyftExpressionExactSearch("To be, or not to be"));
    private static final String JSON_CONTENT = "{\"results\":[{\"_index\":{\"file\":\"/elasticsearch/elasticsearch/nodes/0/indices/shakespeare/0/index/_0.shakespearejsonfld\",\"offset\":7093566,\"length\":199,\"fuzziness\":0,\"host\":\"ryftone-310\"},\"_uid\":\"34229\",\"doc\":{\"line_id\":34230,\"line_number\":\"3.1.64\",\"play_name\":\"Hamlet\",\"speaker\":\"HAMLET\",\"speech_number\":19,\"text_entry\":\"To be, or not to be: that is the question:\"},\"type\":\"line\"}"
            + "],\"stats\":{\"matches\":1,\"totalBytes\":18893841,\"duration\":200,\"dataRate\":90.0928544998169,\"fabricDuration\":48,\"fabricDataRate\":375.386871,\"host\":\"ryftone-310\"}}";

    private static final String JSON_ERORS_IN_CONTENT = "{\"errors\":[\"Ryft Error\"],\"results\":[],\"stats\":{\"matches\":1,\"totalBytes\":18893841,\"duration\":200,\"dataRate\":90.0928544998169,\"fabricDuration\":48,\"fabricDataRate\":375.386871,\"host\":\"ryftone-310\"}}";

    private static final String JSON__ERROR_IN_FIELD_CONTENT = "{\"results\":[{\"_error\":\"failed to parse JSON data: invalid character '\\\\x1a' after object key\" , \"_index\":{\"file\":\"/elasticsearch/elasticsearch/nodes/0/indices/shakespeare/0/index/_0.shakespearejsonfld\",\"offset\":7093566,\"length\":199,\"fuzziness\":0,\"host\":\"ryftone-310\"}}],\"stats\":{\"matches\":1,\"totalBytes\":18893841,\"duration\":200,\"dataRate\":90.0928544998169,\"fabricDuration\":48,\"fabricDataRate\":375.386871,\"host\":\"ryftone-310\"}}";

    private static final String[] fails = new String[1];

    private EmbeddedChannel channel;
    private RestClientHandler handler;
    @Inject
    public RyftRequestEventFactory ryftRequestEventFactory;
    private RyftRequestEvent event;
    ActionListener<SearchResponse> listener;

    @Before
    public void init() {
        fails[0] = "Ryft Error";
        Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                install(new JSR250Module());
                bind(RyftRequestEventFactory.class).toProvider(
                        FactoryProvider.newFactory(RyftRequestEventFactory.class, RyftRequestEvent.class));
            }
        }).injectMembers(this);
        event = ryftRequestEventFactory.create(RYFT_QUERY);

        listener = mock(ActionListener.class);
        event.setIndex((String[]) Arrays.asList("shakespeare").toArray());
        event.setCallback(listener);

        handler = new RestClientHandler(event);
        channel = new EmbeddedChannel(handler);
        channel.pipeline().addFirst(new HttpResponseDecoder());
    }

    @Test
    public void testChannelRead() throws Exception {
        DefaultHttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK);
        channel.writeInbound(msg);
        List<String> names = channel.pipeline().names();
        assertTrue(names.contains("RestClientHandler#0"));

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void testWritingContent() throws Exception {
        DefaultHttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK);
        channel.writeInbound(msg);
        assertTrue(handler.accumulator != null);
        ByteBuf content = Unpooled.buffer(JSON_CONTENT.getBytes().length);
        content.writeBytes(JSON_CONTENT.getBytes());
        DefaultHttpContent httpContent = new DefaultHttpContent(content);
        channel.writeInbound(httpContent);
        assertTrue(handler.accumulator.readableBytes() == JSON_CONTENT.getBytes().length);
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void testWritingPartialContent() throws Exception {
        DefaultHttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK);
        channel.writeInbound(msg);
        assertTrue(handler.accumulator != null);

        String part1 = JSON_CONTENT.substring(0, 300);
        String part2 = JSON_CONTENT.substring(300, 400);
        String part3 = JSON_CONTENT.substring(400);

        ByteBuf content = Unpooled.buffer(part1.getBytes().length);
        content.writeBytes(part1.getBytes());
        DefaultHttpContent httpContent = new DefaultHttpContent(content);
        channel.writeInbound(httpContent);
        assertTrue(handler.accumulator.readableBytes() == part1.getBytes().length);

        content = Unpooled.buffer(part2.getBytes().length);
        content.writeBytes(part2.getBytes());
        httpContent = new DefaultHttpContent(content);
        channel.writeInbound(httpContent);
        assertTrue(handler.accumulator.readableBytes() == (part1 + part2).getBytes().length);

        content = Unpooled.buffer(part3.getBytes().length);
        content.writeBytes(part3.getBytes());
        httpContent = new DefaultHttpContent(content);
        channel.writeInbound(httpContent);
        assertTrue(handler.accumulator.readableBytes() == (part1 + part2 + part3).getBytes().length);

        channel.close();

        verify(listener, times(1)).onResponse(any());

        ReferenceCountUtil.release(msg);
    }

    @Test
    public void testErorsInContent() throws Exception {

        List<ShardSearchFailure> failures = new ArrayList<ShardSearchFailure>();
        for (String failure : fails) {
            failures.add(new ShardSearchFailure(new RyftRestExeption(failure), null));
        }
        SearchResponse expectedResp = new SearchResponse(InternalSearchResponse.empty(), null, 1, 0, 0,
                failures.toArray(new ShardSearchFailure[fails.length]));
        ActionListener<SearchResponse> callback = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                assertEquals(expectedResp.getShardFailures().length, response.getShardFailures().length);
                assertEquals(expectedResp.getShardFailures()[0].status(), response.getShardFailures()[0].status());
            }

            @Override
            public void onFailure(Throwable e) {
                // TODO Auto-generated method stub
            }
        };

        event.setCallback(callback);

        DefaultHttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK);
        channel.writeInbound(msg);
        assertTrue(handler.accumulator != null);
        ByteBuf content = Unpooled.buffer(JSON_ERORS_IN_CONTENT.getBytes().length);
        content.writeBytes(JSON_ERORS_IN_CONTENT.getBytes());
        DefaultHttpContent httpContent = new DefaultHttpContent(content);
        channel.writeInbound(httpContent);
        assertTrue(handler.accumulator.readableBytes() == JSON_ERORS_IN_CONTENT.getBytes().length);
        channel.close();
        ReferenceCountUtil.release(msg);
    }

    @Test
    public void testErorsWithFields() throws Exception {
        ActionListener<SearchResponse> callback = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                assertTrue(new String(response.getHits()
                        .getAt(0)
                        .source())
                        .equals("{\"error\": \"failed to parse JSON data: invalid character '\\x1a' after object key\"}"));
            }

            @Override
            public void onFailure(Throwable e) {
            }
        };

        event.setCallback(callback);

        DefaultHttpResponse msg = new DefaultHttpResponse(HttpVersion.HTTP_1_0, HttpResponseStatus.OK);
        channel.writeInbound(msg);
        assertTrue(handler.accumulator != null);
        ByteBuf content = Unpooled.buffer(JSON__ERROR_IN_FIELD_CONTENT.getBytes().length);
        content.writeBytes(JSON__ERROR_IN_FIELD_CONTENT.getBytes());
        DefaultHttpContent httpContent = new DefaultHttpContent(content);
        channel.writeInbound(httpContent);
        assertTrue(handler.accumulator.readableBytes() == JSON__ERROR_IN_FIELD_CONTENT.getBytes().length);
        channel.close();
        ReferenceCountUtil.release(msg);
    }
}
