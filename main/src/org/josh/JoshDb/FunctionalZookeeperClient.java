package org.josh.JoshDb;

import org.apache.zookeeper.*;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class FunctionalZookeeperClient implements Closeable, Watcher
{
    private final RemoteNodeStore remotes;
    private final UUID nodeId;
    private ZooKeeper zk;

    private static final Logger logger = LoggerFactory.getLogger(FunctionalZookeeperClient.class);

    private final NonBlockingHashMap<String, Object> cached = new NonBlockingHashMap<>();
    private final ScheduledExecutorService nodeUpdaterService =
            Executors.newScheduledThreadPool(1);
    private Thread serverThread;

    private AtomicReference<ExposedQuorumPeer> unprotectedZookeeper = new AtomicReference<>();

    private final int zookeeperPort;

    public FunctionalZookeeperClient(Path remoteNodeStorePath, UUID nodeId) throws IOException, QuorumPeerConfig.ConfigException
    {
        this(new RemoteNodeStore(remoteNodeStorePath), nodeId);
    }

    public FunctionalZookeeperClient(String remoteNodeStoreName, UUID nodeId) throws IOException, QuorumPeerConfig.ConfigException
    {
        this(new RemoteNodeStore(remoteNodeStoreName), nodeId);
    }

    public FunctionalZookeeperClient(UUID nodeId) throws IOException, QuorumPeerConfig.ConfigException
    {
        this(new RemoteNodeStore(), nodeId);
    }

    public FunctionalZookeeperClient(RemoteNodeStore remotes, UUID nodeId) throws IOException, QuorumPeerConfig.ConfigException
    {
        this.nodeId = nodeId;

        // so step one we save off the RemoteNodes start the zookeeper
        // client using the RemoteNodes
        this.remotes = remotes;

        this.zookeeperPort = findUsableZookeeperPort();

        startZookeeperClient
        (
            remotes
                .getNodes()
                .stream()
                .map(RemoteNode::getHostPortPair)
                .collect(Collectors.toList())
        );


        // then we set up a job to update the RemoteNodes periodically now that
        // we've set up the zookeeper client and can use the zookeeper client to
        //retrieve nodes for the zookeeper client to connect to
        nodeUpdaterService.scheduleAtFixedRate
        (
            () ->
            {
                try
                {
                    this.remotes.updateNodes(getString(SettingNames.nodeList));
                }catch (Throwable t)
                {
                    logger.warn("Failed to update remote nodes", t);
                }
            },
            0,
            2,
            TimeUnit.MINUTES
        );

        //then we set up the server
        long zookeeperId = nodeId.getLeastSignificantBits();

        Properties properties = new Properties();

        // so now we just need to set the properties required for
        // the zookeeper server

        // so step one (also easiest step) we make a data dir
        // based on the node id and then set up the myid file and the
        // available hosts based on on org.josh.JoshDb.RemoteNodeStore

        Path localDataDir = Paths.get("./data/" + nodeId.toString());

        try
        {
            Files.createDirectories(localDataDir.getParent());
        }catch (IOException e)
        {
            //it's probably fine
        }

        try
        {
            Path idFile = localDataDir.resolve("myid");
            if (!Files.exists(idFile.getParent()))
            {
                Files.createDirectories(idFile.getParent());
            }

            if (!Files.exists(idFile))
            {
                Files.createFile(idFile);
            }

            try(BufferedWriter writer = Files.newBufferedWriter(idFile))
            {
                writer.write(Long.toString(zookeeperId));
//                writer.newLine();
            }
        }catch (IOException e)
        {
            logger.error("Error writing to IDFile", e);
            //it's probably fine
        }

        int clientPort = findUsableZookeeperPort();

        properties.setProperty("dataDir", localDataDir.toAbsolutePath().toString());
        properties.setProperty("clientPortAddress", "localhost.localdomain");
        properties.setProperty("clientPort", Integer.toString(clientPort));


        //ok, so the key is "server.[sid]"
        //and then the value (we're going to ignore what to do
        // if you're including an ipv6 hostname) is "[hostname]:[port](:[electionPort])?
        // and for now we're going to ignore the trailing bit that lets you specify
        // the peerType because for now everyone's going to be a participant
        //so let's see what happens if we just specify hostname and port
        remotes
            .getNodesStream()
            .map(this::keyValArrayForRemoteNode)
            .forEach(keyVal -> setPropertyFromKeyValArray(properties, keyVal));

        //Zookeeper expects us to have self in the list of servers,
        // TODO do this programmatically instead of manually
        RemoteNode selfAsRemote = getSelfAsRemoteNode();

        setPropertyFromKeyValArray(properties, keyValArrayForRemoteNode(selfAsRemote));


        runZookeeperServerFromProperties(properties);
    }

    private RemoteNode getSelfAsRemoteNode()
    {
        return new RemoteNode("localhost", zookeeperPort, nodeId);
    }

    private void runZookeeperServerFromProperties(Properties properties) throws IOException, QuorumPeerConfig.ConfigException
    {
        QuorumPeerConfig config = new QuorumPeerConfig();
        config.parseProperties(properties);

        serverThread = new Thread(() ->
        {
            try
            {
                unprotectedZookeeper.set(new ExposedQuorumPeer());
                unprotectedZookeeper.get().runFromConfig(config);
            }
            catch (Throwable t)
            {
                logger.error("Error starting zookeeper service", t);
            }
        });

        // then once we have the server all set up (hopefully) we set the server
        // thread off and running
        serverThread.start();
    }

    private void setPropertyFromKeyValArray(Properties properties, String[] keyVal)
    {
        properties.setProperty(keyVal[0], keyVal[1]);
    }

    private String[] keyValArrayForRemoteNode(RemoteNode remoteNode)
    {
        return new String[]
        {
            "server." + remoteNode.getId().getLeastSignificantBits(),
            remoteNode.getHostPortPair().host + ":" + remoteNode.getHostPortPair().port
            //this hacky collect makes me long for the scala `->`
        };
    }

    private class ExposedQuorumPeer extends QuorumPeerMain
    {
        QuorumPeer unprotected;

        ExposedQuorumPeer() throws SaslException
        {
            super();
            unprotected = super.quorumPeer;
        }
    }

    public void close() throws IOException
    {
        try
        {
            zk.close();

            ExposedQuorumPeer unprotected = unprotectedZookeeper.get();

            if (unprotected.unprotected != null)
            {
                unprotected.unprotected.shutdown();
            }

            if (serverThread != null)
            {
                serverThread.interrupt();
            }

            //TODO this is heinous, fix it
            Thread.sleep(4000);

            if (serverThread != null && serverThread.isAlive())
            {
                serverThread.stop();
            }

            nodeUpdaterService.shutdown();
        }
        catch (InterruptedException e)
        {
            InterruptedIOException ioException = new InterruptedIOException();
            ioException.initCause(e);
            ioException.setStackTrace(Thread.getAllStackTraces().get(Thread.currentThread()));
            throw ioException;
        }
    }

    private int findUsableZookeeperPort()
    {
        return 1025;
    }

    public void startZookeeperClient(List<HostPortPair> nodes) throws IOException
    {
        startZookeeperClient(HostPortPair.toConnectionString(nodes));
    }

    private void startZookeeperClient(String connectionString) throws IOException
    {
        zk = new ZooKeeper(connectionString, 3000, this);
    }

    public String getString(String path) throws KeeperException, InterruptedException
    {
        return getString(path, false);
    }

    public Long getLong(String path) throws KeeperException, InterruptedException
    {
        return getLong(path, false);
    }

    private static final Charset serdeCharset = Charset.forName("UTF-8");

    // TODO make sure we use exactly one charset here and detect the
    // local charset and convert if necessary
    private String getString(String path, boolean force) throws KeeperException, InterruptedException
    {
        if (!force)
        {
            String cachedValue = (String) cached.get(path);
            if (cachedValue != null)
            {
                return cachedValue;
            }
        }

        byte[] value = zk.getData(path, this, null);
        String stringValue = new String(value, serdeCharset);

        cached.put(path, stringValue);

        return stringValue;
    }

    private Long getLong(String path, boolean force) throws KeeperException, InterruptedException
    {
        if (!force)
        {
            Long cachedValue = (Long) cached.get(path);
            if (cachedValue != null)
            {
                return cachedValue;
            }
        }

        byte[] value = zk.getData(path, this, null);
        Long longValue = ByteBuffer.allocate(8).put(value).getLong();

        cached.put(path, longValue);

        return longValue;
    }



    public CompletableFuture<Long> setLong(String path, Long value)
    {
        CompletableFuture<Long> valueSetFuture = new CompletableFuture<>();

        AsyncCallback.StatCallback statCallback = (rc, path1, ctx, stat) ->
        {
            KeeperException exception = KeeperException.create(KeeperException.Code.get(rc));

            switch (exception.code())
            {
                case OK:
                    cached.put(path1, value);
                    valueSetFuture.complete(value);
                default:
                    valueSetFuture.completeExceptionally(exception);
            }
        };

        //todo I don't feel like dealing with versioning just now, but I probably
        // should at some point
        zk.setData(path,
                ByteBuffer.allocate(8).putLong(value).array(),
                -1,
                statCallback,
                null);

        return valueSetFuture;
    }

    public CompletableFuture<String> setString(String path, String value)
    {
        CompletableFuture<String> valueSetFuture = new CompletableFuture<>();

        AsyncCallback.StatCallback statCallback = (rc, path1, ctx, stat) ->
        {
            KeeperException exception = KeeperException.create(KeeperException.Code.get(rc));

            switch (exception.code())
            {
                case OK:
                    cached.put(path1, value);
                    valueSetFuture.complete(value);
                default:
                    valueSetFuture.completeExceptionally(exception);
            }
        };

        //todo I don't feel like dealing with versioning just now, but I probably
        // should at some point
        zk.setData(path,
                value.getBytes(serdeCharset),
                -1,
                statCallback,
                null);

        return valueSetFuture;
    }



    // todo do I now need to write another function wrapping
    // this one to deal with these exceptions?
    public Object runWithArguments
    (
        FunctionalZooKeeperInterface action,
        List<String> stringArgumentPaths,
        List<String> longArgumentPaths,
        Set<Class<? extends Throwable>> recoverableExceptions
    )
    throws KeeperException, InterruptedException
    {
        HashMap<String, String> stringArguments = new HashMap<>();
        HashMap<String, Long> longArguments = new HashMap<>();
        Throwable caught = null;
        Object retVal = null;

        do
        {
            for (String i : stringArgumentPaths)
            {
                stringArguments.put(i, getString(i));
            }

            for (String i : longArgumentPaths)
            {
                longArguments.put(i, getLong(i));
            }

            try
            {
                retVal = action.withArguments(stringArguments, longArguments);
            }
            catch (Throwable t)
            {
                caught = t;
            }

        } while
        (
            caught != null
            &&
            (
                recoverableExceptions != null
                &&
                recoverableExceptions.contains(caught.getClass())
            )
        );


        return retVal;
    }

    @Override
    public void process(WatchedEvent watchedEvent)
    {
        Event.EventType type = watchedEvent.getType();

        try
        {
            switch (type)
            {
                case NodeDataChanged:
                    String path = watchedEvent.getPath();
                    Object cachedValue = cached.get(path);

                    if (cachedValue != null)
                    {
                        if (cachedValue.getClass().equals(Long.class))
                            cached.put(path, getLong(path, true));
                        else if (cachedValue.getClass().equals(String.class))
                            cached.put(path, getString(path, true));
                    }
                    break;
            }
        }catch (InterruptedException e)
        {
            // different unchecked exceptions because to keep me from fat
            // fingering intellij and merging these two catch blocks
            //unchecked because I'm not sure I want this method to throw exceptions
            // todo
            e.printStackTrace();
            throw new ArrayIndexOutOfBoundsException();
        }
        catch (KeeperException e)
        {
            // todo
            e.printStackTrace();
            throw new NullPointerException();
        }
    }
}
