package org.josh.JoshDB;

import io.netty.buffer.ByteBuf;

import static org.josh.JoshDB.MessageType.QUANTITY_INQUIRY;
import static org.josh.JoshDB.MessageType.QUANTITY_OFFER;
import static org.josh.JoshDB.MessageType.QUANTITY_REQUEST;

public class QuantityMessageFactory implements MessageFactory
{
    @Override
    public Message fromByteBuf(ByteBuf buf, MessageType type)
    {
        switch (type)
        {
            case QUANTITY_INQUIRY:
                return new QuantityInquiryMessage();

            case QUANTITY_OFFER:
                if (buf.readableBytes() < 8)
                {
                    return null;
                }

                return new QuantityOfferMessage(buf.readLong());

            case QUANTITY_REQUEST:
                if (buf.readableBytes() < 8)
                {
                    return null;
                }

                return new QuantityRequestMessage(buf.readLong());

            default:
                return null;
        }
    }



    static class QuantityInquiryMessage implements Message
    {
        @Override
        public void serialize(ByteBuf out)
        {
            out.writeInt(QUANTITY_INQUIRY.ordinal());
        }

        @Override
        public MessageType getType()
        {
            return QUANTITY_INQUIRY;
        }
    }

    static class QuantityOfferMessage implements Message
    {
        final long onOffer;
        QuantityOfferMessage(long onOffer)
        {
            this.onOffer = onOffer;
        }

        @Override
        public void serialize(ByteBuf out)
        {
            out.writeInt(QUANTITY_OFFER.ordinal());
            out.writeLong(onOffer);
        }

        @Override
        public MessageType getType()
        {
            return QUANTITY_OFFER;
        }
    }

    static class QuantityRequestMessage implements Message
    {
        long requestAmount;

        QuantityRequestMessage(long requestAmount)
        {
            this.requestAmount = requestAmount;
        }

        @Override
        public void serialize(ByteBuf out)
        {
            out.writeInt(QUANTITY_REQUEST.ordinal());
            out.writeLong(requestAmount);
        }

        @Override
        public MessageType getType()
        {
            return QUANTITY_REQUEST;
        }
    }
}
