package org.josh.JoshDb;

import org.josh.JoshDb.request.QuantityRequest;
import org.josh.JoshDb.request.Request;

/**
 * Convenience class with a bunch of static methods for org.josh.JoshDb.Message
 */
public class Messages {

    public static Message receipt(Message of, Node thisNode)
    {
        Message copy = new Message();
        copy.receiptChain = of.receiptChain;
        copy.data = of;
        copy.type = Message.MessageType.RECEIPT;
        of.receiptChain =
            Receipt.nonModifyingAppend
            (
                of.receiptChain,
                Receipt.checkPlease(thisNode)
            );

        return copy;
    }

    public static Message intent(Message responseToBe, Node self)
    {
        Message intent = new Message();
        intent.receiptChain = responseToBe.receiptChain;
        intent.data = responseToBe;
        intent.type = Message.MessageType.INTENT;

        setMessageSentInfo(intent, self);

        return intent;
    }

    //node is the node announcing itself
//    public static Message announcement(Node node)
//    {
//        Message announcement = new Message();
//
//        announcement.type = MessageType.ANNOUNCE;
//
//        announcement.sentByNodeId = node.getNodeId();
//        setMessageSentTime(announcement);
//
//        return announcement;
//    }

    private static void setMessageSentInfo(Message message, Node node)
    {
        message.sentByNodeId = node.getNodeId();
        setMessageSentTime(message);
    }


    private static void setMessageSentTime(Message message)
    {
        message.sentAt = System.currentTimeMillis();
    }

    public static Message ack(Message msg, Node thisNode)
    {
        Message ack = new Message();

        ack.type = Message.MessageType.ACK;
        ack.receiptChain = msg.receiptChain;
        ack.receiptChain =
            Receipt.nonModifyingAppend
            (
                msg.receiptChain,
                Receipt.checkPlease(thisNode)
            );
        ack.data = msg.messageId;

        return ack;
    }


    public static Message quantityRequestMessage
    (
        QuantityRequest.QuantityRequestType type,
        long requestAmount
    )
    {
        Message requestMessage = new Message();

        requestMessage.type = Message.MessageType.REQUEST;

        QuantityRequest request = new QuantityRequest();

        request.type = type;
        request.requestAmount = requestAmount;

        requestMessage.data = request;

        return requestMessage;
    }
}
