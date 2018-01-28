package org.josh.JoshDb;

import java.util.List;
import java.util.UUID;

enum MessageType
{
    ANNOUNCE,
    INTENT,
    ACK, RECEIPT
}

public class Message {
    UUID messageId;

    long sentAt;
    //this will become the certificate of the org.josh.JoshDb.Node
    UUID sentByNodeId;

    List<Receipt> receiptChain; //will be blank when first sending

    Object data;

    MessageType type;
}
