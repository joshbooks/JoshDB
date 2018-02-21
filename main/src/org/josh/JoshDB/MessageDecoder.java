package org.josh.JoshDB;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class MessageDecoder extends ByteToMessageDecoder
{
    private final MessageFactory factory ;

    public MessageDecoder() {this(new MessageFactorySwitch());}
    public MessageDecoder(MessageFactory factory)
    {
        this.factory = factory;
    }

    @Override
    protected void decode
    (
        ChannelHandlerContext channelHandlerContext,
        ByteBuf byteBuf,
        List<Object> list
    ) throws Exception
    {
        MessageFactory.Message msg = factory.fromByteBuf(byteBuf, null);

        if (msg != null)
        {
            list.add(msg);
        }
    }
}
