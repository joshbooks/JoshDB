import java.util.HashMap;
import java.util.UUID;

public class RemoteNode {

    private final UUID id;

    private String host;
    private int port;

    public static HashMap<UUID, Node> hackyBullshit = new HashMap<>();

    public RemoteNode(String host, int port, UUID id)
    {
        this.id = id;
    }

    public void sendMessage(Message msg)
    {
        //todo this is not so great. The RemoteNode should store the type of
        //connection this Node has to the RemoteNode and call the appropriate
        // function to send the message over that type of connection. but for
        // now since we're going to be testing this all on one achine, we'll
        // just assume that all nodes are connected by being run on the same JVM
        //and registering with some hackyBullshit to make this work for now

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
