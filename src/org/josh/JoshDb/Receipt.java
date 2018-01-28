package org.josh.JoshDb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Receipt {
    long receivedAt;
    //this will become the certificate of the receipt
    UUID receivedBy;

    public Receipt(Receipt toCopy)
    {
        this.receivedAt = toCopy.receivedAt;
        this.receivedBy = UUID.fromString(toCopy.receivedBy.toString());
    }

    public Receipt(Node node)
    {
        receivedAt = System.currentTimeMillis();
        receivedBy = UUID.fromString(node.getNodeId().toString());
    }

    public static List<Receipt> nonModifyingAppend(List<Receipt> existing, Receipt addition)
    {
        List<Receipt> appendedList = new ArrayList<>();

        existing.stream().map(Receipt::new).forEach(appendedList::add);

        appendedList.add(addition);

        return appendedList;
    }
}
