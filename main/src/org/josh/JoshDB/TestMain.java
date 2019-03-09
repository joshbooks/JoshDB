package org.josh.JoshDB;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.function.Consumer;

public class TestMain
{
    static void runWithMessagePersistor(Consumer<MessagePersistor> consumer) throws IOException
    {
        MessagePersistor persistor = new MessagePersistor(Paths.get("/home/flatline/Desktop/testLog"));

        consumer.accept(persistor);

        persistor.shutDown();
    }

//    public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException, KeeperException
//    {
//        Node firstnode = new Node();
//        firstnode.
//
//    }

    //todo I broke this a little, not a lot, should fix
//    public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException, KeeperException, QuorumPeerConfig.ConfigException
//    {
//        Node testNode = new Node();
//        runWithMessagePersistor
//        (
//                persistor ->
//                {
//                    org.josh.JoshDB.Message toSend = new org.josh.JoshDB.Message();
//
//                    toSend.type =  org.josh.JoshDB.Message.MessageType.ACK;
//                    toSend.data = "hi";
//                    toSend.receiptChain = new ArrayList<>();
//                    toSend.receiptChain.add(new Receipt(testNode));
//
//                    persistor.persistMessage
//                    (
//                            toSend, msg ->
//                            {
//                                System.out.println(msg.type);
//                                // todo it is not ok at all that we're allowing arbitrary
//                                // Objects to be passed with messages, how are we going to
//                                // serde that safely? What all do we really need to pass as
//                                // message data? What might we conceivably want to pass as
//                                // message data that can't be represented as a nested map
//                                // structure? It seems like it should be easy to detect
//                                // cyclical references and disallow types that have methods
//                                System.out.println(msg.data);
//                                System.out.println
//                                        (
//                                            msg.receiptChain == null
//                                            ?
//                                                null
//                                            :
//                                                msg
//                                                    .receiptChain
//                                                    .stream()
//                                                    .map(org.josh.JoshDB.Receipt::toString)
//                                                    .collect(Collectors.joining())
//                                        );
//
//                            }
//                    );
//                }
//        );
//
//        Thread.sleep(10 * 1000);
//
//        testNode.close();
//
//        System.exit(2);
//    }

//    public static void main(String[] args) throws IOException
//    {
//
//    }

}
