package com.dataart.ryft.elastic.plugin.rest.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class NettyUtils {
    
    public static <T> void setAttribute( AttributeKey<T> attrName, T value, Channel channel){
        channel.attr(attrName).set(value);
    }
    
    public static <T> void setAttribute( AttributeKey<T> attrName, T value, ChannelHandlerContext ctx){
        ctx.channel().attr(attrName).set(value);
    }

    public static <T> T getAttribute(ChannelHandlerContext ctx, AttributeKey<T> key) {
        return getAttribute(ctx.channel(), key);
    }

    public static <T> T getAttribute(Channel channel, AttributeKey<T> key) {
        Attribute<T> attr = channel.attr(key);
        return attr == null ? null : attr.get();
    }

}
