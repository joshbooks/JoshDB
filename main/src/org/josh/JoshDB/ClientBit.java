package org.josh.JoshDB;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class ClientBit extends CommonNodeConnectyBit
{
    private final Bootstrap bootstrap;

    public ClientBit(Kiss kiss)
    {
        super(kiss);
        //it's weird that this doesn't look like ServerBit, stupid example code :/
        //todo make uniform
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.handler(channelInitializer);
    }

    @Override
    public void writeMessage
    (
        MessageFactory.Message msg,
        Consumer<MessageFactory.Message> onWrite,
        InetSocketAddress remote
    )
    {
        try
        {
            super.writeMessage(msg, onWrite, remote);
        } catch (IllegalArgumentException e)
        {
            connectToRemoteAndWrite(msg, onWrite, remote);
        }
    }

    public void connectToRemoteAndWrite
    (
        MessageFactory.Message msg,
        Consumer<MessageFactory.Message> onWrite,
        InetSocketAddress remote
    )
    {
        ChannelFuture future = connectToRemote(remote);
        future.addListener
        (
            future1 ->
            {
                ((ChannelFuture)future1).channel();
                writeToChannel
                (
                    future.channel(),
                    msg,
                    onWrite
                );
            }
        );
    }

    public ChannelFuture connectToRemote(InetSocketAddress remote)
    {
        final ChannelFuture future = bootstrap.connect(remote);
        future.addListener
        (
            aVoid ->
            {
                channelForRemote.put(remote, future.channel());
            }
        );

        return future;
    }
}
