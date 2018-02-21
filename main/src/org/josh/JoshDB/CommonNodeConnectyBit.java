package org.josh.JoshDB;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.FutureListener;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CommonNodeConnectyBit
{
    protected final EventLoopGroup bossGroup;
    protected final EventLoopGroup workerGroup;
    protected final ChannelInitializer<SocketChannel> channelInitializer =
        new ChannelInitializer<SocketChannel>()
        {
            @Override
            public void channelActive(final ChannelHandlerContext ctx) throws Exception
            {
                System.out.println("Channel active");
                System.out.println("channel to " + ctx.channel().remoteAddress() + "is active");
                super.channelActive(ctx);
            }

            @Override
            public void initChannel(SocketChannel ch) throws Exception
            {
                InetSocketAddress remote = ch.remoteAddress();

                if (remote != null)
                {
                    System.out.println("remote wasn't null, it was: " + remote);
                    channelForRemote.put(remote, ch);
                }

                System.out.println("Now initializing a channel: "+ch+"to remote:"+remote);

                ch.pipeline().addLast
                (
                    new MessageDecoder(),
                    new InboundHandlerBit(kiss, remote)
                );

                ch.pipeline().addLast
                (
                    new MessageEncoder(),
                    new OutboundHandlerBit()
                );
            }
        };
    protected Kiss kiss;
    protected NonBlockingHashMap<InetSocketAddress, Channel> channelForRemote =
            new NonBlockingHashMap<>();

    private Thread flusherThread =
        new Thread
        (
            () ->
            {
                while (true)
                {
                    try
                    {
                        Thread.sleep(1000*3);
                        flushAll();
                    }
                    catch (Throwable t)
                    {
                        if (t.getClass() == InterruptedException.class)
                        {
                            break;
                        }

                        System.out.println(t.getMessage());
                        t.printStackTrace();
                    }
                }
            }
        );

    public CommonNodeConnectyBit(Kiss kiss)
    {
        this.kiss = kiss;
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
//        flusherThread.start();
    }

    public void writeMessage
    (
        MessageFactory.Message msg,
        Consumer<MessageFactory.Message> onWrite,
        InetSocketAddress remote
    )
    {
        Channel channel = channelForRemote.get(remote);
        if (channel == null)
        {
            throw new IllegalArgumentException
            (
                "This ServerBit doesn't have a Channel open for remote: " + remote
            );
        }

        writeToChannel(channel, msg, onWrite);
    }

    protected ChannelFuture writeToChannel
    (
        Channel channel,
        MessageFactory.Message msg,
        Consumer<MessageFactory.Message> onWrite
    )
    {
        return channel.writeAndFlush(msg)
            .addListener
            (
                future ->
                {
                    Channel ch = ((ChannelFuture)future).channel();
                    onWrite.accept(msg);
                }
            );
    }

    private void flushAll()
    {
        //we might want to keep around some kind of list of flush actions
        //so we can do things asynchronously on flush
        channelForRemote.values().forEach(Channel::flush);
    }

    public void flush(InetSocketAddress remote)
    {
        channelForRemote.get(remote).flush();
    }

    void close()
    {
        List<ChannelFuture> closeList =
            channelForRemote
                .values()
                .stream()
                .map(ChannelOutboundInvoker::close)
                .collect(Collectors.toList());

        closeList.forEach(ChannelFuture::awaitUninterruptibly);

        flusherThread.interrupt();

        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
}
