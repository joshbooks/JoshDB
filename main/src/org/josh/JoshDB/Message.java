package org.josh.JoshDB;

import io.netty.buffer.ByteBuf;

interface MessageFactory
{
    interface Message
    {
        /**
         * Put the information from this class into a {@link ByteBuf}
         * so we can send it over the network
         * @param out the thing we're going to send over the network
         */
        void serialize(ByteBuf out);

        MessageType getType();
    }

    /**
     * @param buf A {@link ByteBuf} we got over the network,
     *            return null if the ByteBuf does not contain the full message.
     *            type will not be null except in MessageFactorySwitch where we
     *            read the first 4 bytes of the ByteBuf to get the type. Also
     *            after the message has been routed by MessageFactorySwitch the
     *            ByteBuf will be pointing to the 5th byte since it has to read
     *            the first 4 to get the type
     */
    Message fromByteBuf(ByteBuf buf, MessageType type);
}
