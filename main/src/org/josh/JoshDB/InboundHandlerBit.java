package org.josh.JoshDB;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;

public class InboundHandlerBit extends ChannelInboundHandlerAdapter
{
    private final Kiss kiss;
    private final InetSocketAddress remoteAddress;

    public InboundHandlerBit(Kiss kiss, InetSocketAddress remoteAddress)
    {
        this.kiss = kiss;
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
    {
        System.out.println("Channel to "+ctx.channel().remoteAddress()+" is active");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        kiss.onMessage((MessageFactory.Message) msg, remoteAddress);
        // Discard the received data silently.
        ReferenceCountUtil.release(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
