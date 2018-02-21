package org.josh.JoshDB;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.UUID;

/**
 * The master branch is great, and has some good stuff in it.
 * But just to start with what if I keep it stupid simple?
 * What if I just do a distributed implementation of Cliff
 * Click's rather excellent classes
 */
public class Kiss
{
    final InetSocketAddress address;
    //TODO this will have to change to a host port pair so we can actually distribute things
    public final Map<UUID, InetSocketAddress> nodeIdToAddress;
    final ServerBit server;
    private final ClientBit client;
    private final Map<InetSocketAddress, UUID> addressToNode;

    public Kiss
    (
        InetSocketAddress myAddress,
        Map<UUID, InetSocketAddress> nodeIdToAddress
    ) throws SocketException
    {
        this
        (
            myAddress,
            nodeIdToAddress,
            new MapReverse<InetSocketAddress, UUID>()
                .reverseMap(nodeIdToAddress)
        );
    }

    public Kiss
    (
        InetSocketAddress myAddress,
        Map<UUID, InetSocketAddress> nodeIdToAddress,
        Map<InetSocketAddress, UUID> addressToNode
    ) throws SocketException
    {
        this.address = myAddress;
        this.nodeIdToAddress = nodeIdToAddress;
        this.addressToNode = addressToNode;
        //I think it might be possible to just have the client bit bind on
        // the specified port and just leave out the server bit entirely????
        this.server = new ServerBit(this);
        server.startListening(myAddress);
        this.client = new ClientBit(this);
    }

    void onMessage(MessageFactory.Message msg, InetSocketAddress fromNode)
    {
        switch (msg.getType())
        {
            case QUANTITY_INQUIRY:
                System.out.println("received an inquiry from "+fromNode);
                break;
            case QUANTITY_REQUEST:
                QuantityMessageFactory.QuantityRequestMessage rMsg =
                        (QuantityMessageFactory.QuantityRequestMessage) msg;
                System.out.println("received an request for " + rMsg.requestAmount + " from " + fromNode);
                break;
            case QUANTITY_OFFER:
                QuantityMessageFactory.QuantityOfferMessage oMsg =
                        (QuantityMessageFactory.QuantityOfferMessage) msg;
                System.out.println("received an offer of " + oMsg.onOffer + " from " + fromNode);
                break;
            default:
                System.out.println("Unknown message type: "+msg.getType().ordinal());
                break;
        }

    }

    public void sendMessage(MessageFactory.Message msg, UUID otherNode) throws IOException
    {
        InetSocketAddress otherNodeAddress = nodeIdToAddress.get(otherNode);

        if (otherNodeAddress == null)
        {
            throw new IllegalArgumentException("UUID otherNode was not added a peer node to this Kiss");
        }

        try
        {
            server
                .writeMessage
                (
                    msg,
                    (written) ->
                    {
                        System.out.println("just wrote " + msg);
                    },
                    otherNodeAddress
                );
        } catch (Exception e)
        {
            client
                .writeMessage
                (
                    msg,
                    (written) ->
                    {
                        System.out.println("just wrote " + msg);
                    },
                    otherNodeAddress
                );
        }
    }

    public void close()
    {
        server.close();
        client.close();
    }
}
