package org.josh.JoshDB;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageEncoder extends MessageToByteEncoder<MessageFactory.Message>
{
    @Override
    protected void encode(
            ChannelHandlerContext channelHandlerContext, MessageFactory.Message message, ByteBuf byteBuf
    ) throws Exception
    {
        message.serialize(byteBuf);
    }
}
