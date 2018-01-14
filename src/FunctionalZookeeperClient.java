import org.apache.zookeeper.*;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
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
import java.util.stream.Collectors;

public class FunctionalZookeeperClient implements Watcher
{
    private final RemoteNodeStore remotes;
    private ZooKeeper zk;

    private static final Logger logger = LoggerFactory.getLogger(FunctionalZookeeperClient.class);

    private final NonBlockingHashMap<String, Object> cached = new NonBlockingHashMap<>();
    private final ScheduledExecutorService nodeUpdaterService =
            Executors.newScheduledThreadPool(1);

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
        startZookeeperClient
        (
            remotes
                .getNodes()
                .stream()
                .map(RemoteNode::getHostPortPair)
                .collect(Collectors.toList())
        );

        this.remotes = remotes;

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

        long zookeeperId = nodeId.getLeastSignificantBits();

        //todo now that we have the client connected, we can
        //query for the latest list of nodes and use that
        //to generate the config file
        List<RemoteNode> currentRemoteNodes = remotes.getNodes();

        QuorumPeerConfig config = new QuorumPeerConfig();
        Properties properties = new Properties();

        // so now we just need to set the properties required for
        // the zookeeper server

        // so step one (also easiest step) we make a data dir
        // based on the node id and then set up the myid file and the
        // available hosts based on on RemoteNodeStore

        Path localDataDir = Paths.get("./data/" + nodeId.toString());

        try
        {
            Files.createDirectories(localDataDir);
        }catch (IOException e)
        {
            //it's probably fine
        }

        try
        {
            Path idFile = Files.createFile(localDataDir.resolve("myid"));
            BufferedWriter writer = Files.newBufferedWriter(idFile);
            writer.write(Long.toString(zookeeperId));
        }catch (IOException e)
        {
            //it's probably fine
        }


        // todo then we set the properties, starting with the



        config.parseProperties(properties);

        new Thread(() ->
        {
            try
            {
                new QuorumPeerMain().runFromConfig(config);
            }
            catch (IOException e)
            {
                // todo well fuck, what do here?
            }
        }).start();
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
