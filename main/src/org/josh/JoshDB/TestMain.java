package org.josh.JoshDB;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.UUID;

public class TestMain
{
    public static void main(String[] args) throws Exception
    {
        UUID kissUuid = UUID.randomUUID();
        UUID kiss2Uuid = UUID.randomUUID();

        HashMap<UUID, InetSocketAddress> nodesImAboutToCreate = new HashMap<>();

        nodesImAboutToCreate.put(kissUuid, new InetSocketAddress("localhost", 1025));
        nodesImAboutToCreate.put(kiss2Uuid, new InetSocketAddress("localhost", 1026));

        Kiss kiss = new Kiss(nodesImAboutToCreate.get(kissUuid), nodesImAboutToCreate);

        Kiss kiss2 = new Kiss(nodesImAboutToCreate.get(kiss2Uuid), nodesImAboutToCreate);

//        Thread.sleep(30 * 1000);

        kiss2.sendMessage(new QuantityMessageFactory.QuantityOfferMessage(2), kissUuid);

        kiss.sendMessage(new QuantityMessageFactory.QuantityRequestMessage(15), kiss2Uuid);

//        Thread.sleep(30 * 1000);

//        kiss.close();
//        kiss2.close();
    }

}
