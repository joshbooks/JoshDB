package org.josh.JoshDB;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Receipt {
    long receivedAt;
    //this will become the certificate of the receipt
    UUID receivedBy;

    public static Receipt customerCopy(Receipt toCopy)
    {
        Receipt copy = new Receipt();
        copy.receivedAt = toCopy.receivedAt;
        copy.receivedBy = UUID.fromString(toCopy.receivedBy.toString());
        return copy;
    }

    public static Receipt checkPlease(Node node)
    {
        Receipt check = new Receipt();
        check.receivedAt = System.currentTimeMillis();
        check.receivedBy = UUID.fromString(node.getNodeId().toString());
        return check;
    }

    public static List<Receipt> nonModifyingAppend(List<Receipt> existing, Receipt addition)
    {
        List<Receipt> appendedList = new ArrayList<>();

        existing.stream().map(Receipt::customerCopy).forEach(appendedList::add);

        appendedList.add(addition);

        return appendedList;
    }
}
