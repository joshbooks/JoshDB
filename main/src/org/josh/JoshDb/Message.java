package org.josh.JoshDb;

import java.util.List;
import java.util.UUID;



public class Message {
    public enum MessageType
    {
        //ANNOUNCE,
        REQUEST,
        INTENT,
        ACK,
        RECEIPT
    }

    public UUID messageId;

    public long sentAt;
    //this will become the certificate of the org.josh.JoshDb.Node
    public UUID sentByNodeId;

    public List<Receipt> receiptChain; //will be blank when first sending

    public Object data;

    public MessageType type;
}
