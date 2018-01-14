/**
 * Convenience class with a bunch of static methods for Message
 */
public class Messages {

    public static Message receipt(Message of)
    {
        Message copy = new Message();
        copy.receiptChain = of.receiptChain;
        copy.data = of;
        copy.type = MessageType.RECEIPT;

        return copy;
    }

    public static Message intent(Message responseToBe)
    {
        Message intent = new Message();
        intent.receiptChain = responseToBe.receiptChain;
        intent.data = responseToBe;
        intent.type = MessageType.INTENT;

        return intent;
    }

    //node is the node announcing itself
    public static Message announcement(Node node)
    {
        Message announcement = new Message();

        announcement.type = MessageType.ANNOUNCE;

        announcement.sentByNodeId = node.getNodeId();
        setMessageSentTime(announcement);

        return announcement;
    }

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

        ack.type = MessageType.ACK;
        ack.receiptChain = msg.receiptChain;
        Receipt receipt = createReceipt(thisNode);
        ack.receiptChain.add(receipt);
        ack.data = msg.messageId;

        return ack;
    }

    private static Receipt createReceipt(Node thisNode)
    {
        Receipt receipt = new Receipt();

        receipt.receivedAt = System.currentTimeMillis();
        receipt.receivedBy = thisNode.getNodeId();

        return receipt;
    }
}
