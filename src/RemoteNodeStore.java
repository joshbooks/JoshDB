import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemoteNodeStore
{
    private List<RemoteNode> cachedNodes = null;
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

    List<RemoteNode> nodesFromLineStream(Stream<String> lineStream)
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
                        new Integer(triplet[1]),
                        UUID.fromString(triplet[2])
                );
    }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public RemoteNodeStore(Path storeFile) throws IOException
    {
        this.remoteNodeStoreFile = storeFile;
        //so at this point we have the path to the remote nodes setting file
        //if it contains at least one node we succeed otherwise we throw an
        //IOException
        BufferedReader nodeReader = Files.newBufferedReader(storeFile);

        cachedNodes = nodesFromLineStream(nodeReader.lines());

        nodeReader.close();

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
        cachedNodes =
                nodesFromLineStream
                (
                    new ArrayList<>(Arrays.asList(serializedNodes.split("\n")))
                            .stream()
                );
        flushNodesToDisk();
    }
}
