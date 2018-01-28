package org.josh.JoshDb;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class Node implements Closeable {



    // so this guy here is the distributed value thingy,
    // for now there is exactly one of them, and each node is
    // tracking their own little piece of the same distributed
    // value
    //we might want to instead use Cliff Click's Counter class here
    //to allow for better multithreaded performance, but we'll see
    AtomicLong thisNodeValue;

    static final Path logFiles = Paths.get("./logs");

    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    // what kind of info? I don't know, but we're going to need to store some
    // conf stuff/metadata/state, but until we know what that is going to be
    // and in response to what sort of events we're going to be storing
    // and/or reading stuff I have no idea what sort of properties this
    // info file mut have

    // todo ok, so I think one of the configuration values that we're going to
    // need to store, read, and modify is going to be the number of nodes that
    // need to report receiving something for us to proceed, I've defaulted to
    // 3 for now, but there's nothing preventing me from making it 16- or 1
    // for that matter

    // so I think rather than sending the notification to N nodes, we should
    // broadcast it to all of them and just wait for N nodes to respond before
    // proceeding

    // nope, that doesn't scale since it means the more nodes we have the more
    // requests we have to make and requests are not free. Wht I would suggest
    // instead is that we have a constant M independent(ish) of N that
    // specifies the number of nodes to send a request to before waiting for N
    // nodes to ack/respond

    //so this seems like it is secretly a network partitioning problem, so at
    // some point I will need to decide what kinds of networks I want this
    // database to work on and how I'm going to best leverage those different
    // kinds of networks

    private final MessagePersistor logFile;

    private FunctionalZookeeperClient fz;


    // Each node shall have a certificate and then we can
    // just uniquely identify by public key
    private final UUID nodeId;

    final SecureRandom random;

    // todo this is probably going to go away, just use
    // org.josh.JoshDb.FunctionalZookeeperClient/org.josh.JoshDb.RemoteNodeStore
    TreeSet<RemoteNode> nodesOfWhichIAmAware = new TreeSet<>();

    HashMap<UUID, RemoteNode> nodesByUuid;

    public Node()
            throws IOException,
            NoSuchAlgorithmException,
            KeeperException,
            InterruptedException,
            QuorumPeerConfig.ConfigException
    {
        //this(UUID.randomUUID());
        //when starting in standalone mode zookeeper expects a server id of 0
        //todo that's fucked, I don't like it, make it better
        this(new UUID(123, 0));
    }

    public Node(UUID uuid)
            throws IOException,
            NoSuchAlgorithmException,
            KeeperException,
            InterruptedException,
            QuorumPeerConfig.ConfigException
    {
        this.nodeId = uuid;

        logFile = new MessagePersistor(logFiles.resolve(this.nodeId.toString()));
        this.fz = new FunctionalZookeeperClient(this.nodeId);

        //todo decide how to do the thing
        random = new SecureRandom();
        random.setSeed(random.generateSeed(36));

        //before registering this node the constructor should
        //be effectively done, don't put anything after
        // registering the node that might be required in
        // order to respond to requests register for some
        // hackyBullshit TODO this needs to go away
        RemoteNode.hackyBullshit.put(uuid, this);

        // as a replacement let's go ahead and
        // set up the org.josh.JoshDb.FunctionalZookeeperClient
        // and a zookeeper server of some description

        // i think it might make sense to start up the
        // zookeeper on an odd port starting at 101 and then
        // use the next port up (starting at 102) for the
        // JoshDB port, then if either port is taken
        // increment both ports by two

        //the problem is that I'm not sure if it's safe to
        // assume that if the zookeeper server is up that
        // JoshDb server is up
        //meh, they're going to b running as the same process,
        //let's just make sure that





    }

    public void close() throws IOException
    {
        try
        {
            fz.close();
        }
        catch (Throwable t)
        {
            logger.warn("Failed to close the FunctionalZookeperClient", t);
            throw t;
        }
    }

    public UUID getNodeId() {
        return nodeId;
    }

    //I think we might just use zookeeper for node registration
    //todo is that a good idea?
//    public void receiveAnnounce(org.josh.JoshDb.Message announce) throws IOException, NoSuchAlgorithmException {
//        org.josh.JoshDb.Node announcedNode = new org.josh.JoshDb.Node(announce.sentByNodeId);
//
//        // todo initiate/participate in consensus protocol to unanimously agree that this
//        // new node is authenticated
//    }

    // once again I think we're just going to use zookeeper to
    // deal with node registration
//    /**
//     * Announce the existence of this org.josh.JoshDb.Node to the other nodes that we know about
//     */
//    public void announce()
//    {
//        org.josh.JoshDb.Message announcement = org.josh.JoshDb.Messages.announcement(this);
//        for (org.josh.JoshDb.RemoteNode node: nodesOfWhichIAmAware)
//        {
//            sendMessage(node, announcement);
//        }
//    }

    // I guess for now ths is just going to be a stupid simulatory thing,
    // for running all the nodes locally and org.josh.JoshDb.RemoteNode.sendMessage is
    // just going to call org.josh.JoshDb.Node.onMessage using reflection if necessary
    void sendMessage(RemoteNode to, Message msg)
    {
        to.sendMessage(msg);
        //todo
    }

    /**
     * Give away some {@code amount} of stuff from our own local stash of stuff
     * @param amount the amount of stuff to give away
     * @return whether or not it was given
     */
    // TODO this will need to get refactored into a separate class, since
    // we'll have multiple of these and we'll need each org.josh.JoshDb.Node to be able
    // to talk about any/all of them
    boolean giveAway(long amount) {
        ensureSensibleAmount(amount);

        long localValue, amountAfterRequest;

        do
        {
            localValue = thisNodeValue.get();

            if (localValue < amount)
            {
                return false;
            }
            amountAfterRequest = localValue - amount;
        } while (!thisNodeValue.compareAndSet(localValue, amountAfterRequest));

        return true;
    }

    private static void ensureSensibleAmount(long amount)
    {
        //"common sense" limits
        if (amount < 0 || amount >= Long.MAX_VALUE / 2)
        {
            throw new IllegalArgumentException("You know what you did, " +
                                               "and you should feel shame");
        }
    }

    void receive(long amount)
    {
        ensureSensibleAmount(amount);

        thisNodeValue.getAndAdd(amount);
    }


    //ok, so I think this is going to get broken up into little async blocks
    //since there are like 12 network calls and a disk write that need to happen
    //before this function can return as of right now. But it does an excellent job
    //illustrating the flow of how this should work
    void onReceiveRequest(Message request)
    {
        // send out the fact that you got the request and make sure 3 other nodes are aware
        // TODO persist the request
        reportReceipt(request);

        // then try to handle the request
        Message response = processRequest(request);

        //this should be null pretty much just for ack "requests"
        //like this:
        //org.josh.JoshDb.Node 1        |       org.josh.JoshDb.Node 2      |       org.josh.JoshDb.Node 3
        //sendMsg ------------->
        //                    reportReceipt----------->
        //                          <-------------------ack
        if (response == null)
        {
            return;
        }


        // send out your response and make sure it's been received by 3 other nodes
        // TODO persist your response to the request
        reportIntent(response);

        // send response to the node that made the request
        sendMessage(request.sentByNodeId, response);
    }


    private void sendMessage(UUID intendedRecipient, Message msg)
    {
        //todo there are going to be some exceptions we potentially throw here
        //and the exception handling
        RemoteNode recipient = nodesByUuid.get(intendedRecipient);
        sendMessage(recipient, msg);
    }

    //the result of processing a org.josh.JoshDb.Message is another org.josh.JoshDb.Message
    //so do the thing and find the thing the thing that you
    // need to tell people after you've done the thing
    Message processRequest(Message request)
    {
        //todo big shitty switch statement
        switch (request.type)
        {
            case ANNOUNCE:
                //TODO this is going to be a whole thing
                return null;

            case ACK:
                return null;

            case INTENT:
                //todo persist and ack

            case RECEIPT:
                //todo persist and ack

            default:
                throw new IllegalArgumentException(request.type + " is not a recognized type");
        }
    }

    //before we process a request or send a response to a message we're going to
    //send a carbon copy of that message to a number of other nodes
    void sendToOtherNodes(Message msg)
    {
        //so this bit is super basic, node selecting needs to be smartened up a bit
        //like what if there aren't N nodes in the cluster? what if two of the nodes that
        //we randomly select are the same node?
        int[] threeNodeIdxs = new int[3];
        for (int i= 0; i < threeNodeIdxs.length; i++)
            threeNodeIdxs[i] = random.nextInt(nodesOfWhichIAmAware.size());

        HashSet<RemoteNode> threeNodes = new HashSet<>();
        int idx = 0;
        for (RemoteNode i : nodesOfWhichIAmAware)
        {
            for (int j : threeNodeIdxs)
            {
                if (j == idx)
                {
                    threeNodes.add(i);
                }
            }
            idx++;
        }

        sendMessage(threeNodes, msg);
    }

    void sendMessage(HashSet<RemoteNode> to, Message of)
    {
        for (RemoteNode i : to)
        {
            sendMessage(i, of);
        }
    }

    void reportReceipt(Message received)
    {
        Message receipt = Messages.receipt(received);
        sendToOtherNodes(receipt);
    }

    //todo so I think maybe the reason this needs to be a separate function
    //is that intents need to be a separate message type to receipt messages
    //tell people what you're about to tell the person who
    // sent the message that resulted in this message
    void reportIntent(Message responseToBe)
    {
        Message intent = Messages.intent(responseToBe);
        sendToOtherNodes(intent);
    }


}
//each node shall maintain a org.josh.JoshDb.RemoteNode object for other Nodes, with as up
//to date a replica of that org.josh.JoshDb.Node's state as they can figure out from the messages
//they overhear, that way they know who to ask when it comes time to satisfy a request

//In order to keep up to date records about what Nodes you have the best connection with, Nodes
//shall do heartbeats in the background
//org.josh.JoshDb.Node 1   |   org.josh.JoshDb.Node 2
//msg 1 -------->
//  <------------ msg2
//msg1: { helloFrom: org.josh.JoshDb.Node 1, sentAt: timeSent }
//msg2: { helloFrom: org.josh.JoshDb.Node 2, msg1ReceivedAt: timeReceived, sentAt: timeSent }
//the separation of received and sent times in msg2 is to allow this to happen asynchronously
//all org.josh.JoshDb.Node 2 needs to do is mark down when the message was received and submit it to a queue
//for response. This can happen around an outage or be throttled or be scheduled as lowest priority
//and it should still be just as accurate, because all we're looking for here is network speed
//between two Nodes

//deciding that a org.josh.JoshDb.Node or Set of Nodes is inactive shall require consensus between all other Nodes

//deciding that a org.josh.JoshDb.Node is ready to become active shall require consensus between all active Nodes