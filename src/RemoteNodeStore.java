import org.cliffc.high_scale_lib.NonBlockingHashSet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemoteNodeStore
{
    private Set<RemoteNode> cachedNodes = new NonBlockingHashSet<>();
    private final Object cachedNodesLock = new Object();
    private final Path remoteNodeStoreFile;

    public RemoteNodeStore() throws IOException
    {
        this(RemoteNodeStoreSettings.storeFileName);
    }

    public RemoteNodeStore(String storeFileName) throws IOException
    {
        this(RemoteNodeStoreSettings.storeFileDirectory.resolve(storeFileName));
    }

    Stream<RemoteNode> nodeStreamFromLineStream(Stream<String> lineStream)
    {
        return lineStream.map(line -> {
        String[] triplet = line.split(":");
        if (triplet.length != 3)
        {
            return null;
        }

        return new RemoteNode
                (
                        triplet[0],
                        Integer.decode(triplet[1]),
                        UUID.fromString(triplet[2])
                );
        }).filter(Objects::nonNull);
    }


    public Stream<RemoteNode> nodeStreamFromNodeStore(Path storeFile) throws IOException
    {
        try (BufferedReader nodeReader = Files.newBufferedReader(storeFile))
        {
            return nodeStreamFromLineStream(nodeReader.lines());
        }
    }


    public void updateCacheFromNodeStream(Stream<RemoteNode> nodeStream)
    {
        cachedNodes.addAll(nodeStream.collect(Collectors.toList()));
    }

    public void updateCacheFromNodeStore() throws IOException
    {
        updateCacheFromNodeStream(nodeStreamFromNodeStore(remoteNodeStoreFile));
    }



    public RemoteNodeStore(Path storeFile) throws IOException
    {
        this.remoteNodeStoreFile = storeFile;
        //so at this point we have the path to the remote nodes setting file
        //if it contains at least one node we succeed otherwise we throw an
        //IOException
        updateCacheFromNodeStore();

        if (cachedNodes.size() == 0)
        {
            throw new IOException("The file that was specified as the " +
                                  "remote node store file didn't contain " +
                                  "at least one valid host:port:uuid " +
                                  "triplet");
        }
    }

    public List<RemoteNode> getNodes()
    {
        if (cachedNodes == null)
        {
            try
            {
                cachedNodesLock.wait();
            }
            catch (InterruptedException e)
            {
                //todo log something
                return new ArrayList<>();
            }
        }

        return new ArrayList<>(cachedNodes);
    }


    public void flushNodesToDisk() throws IOException
    {
        Path tempFile = temporaryFilePath();
        BufferedWriter writer = Files.newBufferedWriter(tempFile);

        for(RemoteNode node : cachedNodes)
        {
            HostPortPair pair = node.getHostPortPair();
            UUID id = node.getId();

            writer.write(pair.host + ":" + pair.port + ":" + id.toString());
            writer.newLine();
        }

        writer.flush();
        writer.close();

        swapFileWithTempFile(tempFile);
    }

    private void swapFileWithTempFile(Path tempFile) throws IOException
    {
        try
        {
            Files.delete(remoteNodeStoreFile);
        }catch (FileNotFoundException e)
        {
            //it's already gone, no big deal
            //todo log something
        }

        Files.move(tempFile, remoteNodeStoreFile);
    }

    private Path temporaryFilePath()
    {
        return remoteNodeStoreFile.getParent().resolve(uniqueTemporaryName());
    }

    private String uniqueTemporaryName()
    {
        return remoteNodeStoreFile.getFileName()+UUID.randomUUID().toString();
    }

    public void updateNodes(String serializedNodes) throws IOException
    {
        // todo this function is a little gnarly, might want to
        // refactor this and related functions into a StoreIO static
        // util class and make it all nice and streamy and modular
        cachedNodes.addAll(
                nodeStreamFromLineStream
                (
                    new ArrayList<>(Arrays.asList(serializedNodes.split("\n")))
                            .stream()
                ).collect(Collectors.toList()));
        flushNodesToDisk();
    }
}
