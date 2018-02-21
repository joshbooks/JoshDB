package org.josh.JoshDB;

import io.netty.buffer.ByteBuf;

import java.util.EnumMap;

public class MessageFactorySwitch implements MessageFactory
{
    private static final MessageType[] messageTypes = MessageType.values();
    private static final QuantityMessageFactory quantityMessageFactory =
        new QuantityMessageFactory();
    private static final EnumMap<MessageType, MessageFactory> typeToFactory =
        new EnumMap<>(MessageType.class);
    static
    {
        typeToFactory.put(MessageType.QUANTITY_INQUIRY, quantityMessageFactory);
        typeToFactory.put(MessageType.QUANTITY_OFFER, quantityMessageFactory);
        typeToFactory.put(MessageType.QUANTITY_REQUEST, quantityMessageFactory);
    }

    @Override
    public Message fromByteBuf(ByteBuf buf, MessageType unused)
    {
        if (buf.readableBytes() < 4)
        {
            return null;
        }

        int typeInt = buf.readInt();
        MessageType typeEnum = messageTypes[typeInt];

        return factoryForMessageType
        (
            typeEnum
        )
            .fromByteBuf(buf, typeEnum);
    }

    private MessageFactory factoryForMessageType(MessageType type)
    {
        return typeToFactory.get(type);
    }

}
