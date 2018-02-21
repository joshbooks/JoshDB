package org.josh.JoshDB;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

public class ServerBit extends CommonNodeConnectyBit
{

    private final ServerBootstrap bootstrap;

    public ServerBit(Kiss kiss)
    {
        super(kiss);
        this.bootstrap = new ServerBootstrap().group(bossGroup, workerGroup);
    }

    void startListening(InetSocketAddress localAddress)
    {
        new Thread
        (
            () ->
            {
                try
                {
                    listenThreadFunction(localAddress);
                }
                catch (Exception e)
                {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
            }
        )
            .start();
    }

    void listenThreadFunction(InetSocketAddress listenAddress) throws Exception
    {
        bootstrap
            .channel(NioServerSocketChannel.class)
            .childHandler(channelInitializer)
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.
        ChannelFuture f = bootstrap.bind(listenAddress);

        f.addListener
        (
            avoid ->
            {
                System.out.println("server bind future fired");
            }
        );

        f
            .channel()
            .closeFuture()
            .addListener
            (
                avoid ->
                {
                    System.out.println("server bind future channel close future fired");
                }
            );

        // Wait until the server socket is closed.
        // In this example, this does not happen, but you can do that to gracefully
        // shut down your server.
        f.channel().closeFuture().sync();
    }

}
