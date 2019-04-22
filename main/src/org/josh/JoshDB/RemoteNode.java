package org.josh.JoshDB;

import java.util.HashMap;
import java.util.UUID;

public class RemoteNode {

    private final UUID id;

    private String host;
    private int port;

    public static HashMap<UUID, Node> hackyBullshit = new HashMap<>();

    public RemoteNode(String host, int port, UUID id)
    {
        this.host = host;
        this.port = port;
        this.id = UUID.fromString(id.toString());
    }

    public void sendMessage(Message msg)
    {
        // todo this is not so great. The org.josh.JoshDB.RemoteNode should store the type of
        // connection this org.josh.JoshDB.Node has to the org.josh.JoshDB.RemoteNode and call the appropriate
        // function to send the message over that type of connection. but for
        // now since we're going to be testing this all on one machine, we'll
        // just assume that all nodes are connected by being run on the same JVM
        // and registering with some hackyBullshit to make this work for now

        hackyBullshit.get(id).onReceiveRequest(msg);
    }

    public HostPortPair getHostPortPair()
    {
        return new HostPortPair(host, port);
    }

    public UUID getId()
    {
        return id;
    }
}
