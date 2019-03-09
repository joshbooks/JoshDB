package org.josh.JoshDB;

import org.cliffc.high_scale_lib.NonBlockingHashSet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class RemoteNodeStore
{
    private Set<RemoteNode> cachedNodes = new NonBlockingHashSet<>();
    private final Path remoteNodeStoreFile;

    public RemoteNodeStore() throws IOException
    {
        this(RemoteNodeStoreSettings.storeFileName);
    }

    public RemoteNodeStore(String storeFileName) throws IOException
    {
        this(RemoteNodeStoreSettings.storeFileDirectory.resolve(storeFileName));
    }

    public Path storePath()
    {
        return remoteNodeStoreFile;
    }

    public static RemoteNode nodeFromString(String line)
    {
        String[] triplet = line.split(":");
        if (triplet.length != 3)
        {
            return null;
        }

        return new RemoteNode
        (
            triplet[0],
            Integer.parseInt(triplet[1]),
            UUID.fromString(triplet[2])
        );
    }

    public static Stream<RemoteNode> nodeStreamFromLineStream(Stream<String> lineStream)
    {
        return lineStream
                .map(RemoteNodeStore::nodeFromString)
                .filter(Objects::nonNull);
    }

    public static Stream<RemoteNode> nodeStreamFromNodeStore(Path storeFile)
    throws IOException
    {
        if (!Files.exists(storeFile.getParent()))
        {
            Files.createDirectories(storeFile.getParent());
        }
        if (!Files.exists(storeFile))
        {
            Files.createFile(storeFile);
        }

        try (BufferedReader nodeReader = Files.newBufferedReader(storeFile))
        {
            return nodeStreamFromLineStream(nodeReader.lines());
        }
    }

    public void updateCacheFromNodeStream(Stream<RemoteNode> nodeStream)
    {
        try
        {
            cachedNodes.addAll(nodeStream.collect(Collectors.toList()));
        }
        catch (UncheckedIOException e)
        {
            //todo log something
        }
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



        // todo it might make sense for this class to be NodeStore, and tell
        // node what its identity is, since if we start in standalone our
        // node id is expected to be 0
        //unnecessary for now zookeeper automagically starts in standalone
        //node provided there are no observers
//        if (cachedNodes.size() == 0)
//        {
//            throw new IOException("The file that was specified as the " +
//                                  "remote node store file didn't contain " +
//                                  "at least one valid host:port:uuid " +
//                                  "triplet");
//        }
    }

    public List<RemoteNode> getNodes()
    {
        return new ArrayList<>(cachedNodes);
    }


    public void flushCachedNodesToDisk() throws IOException
    {
        Path tempFile = temporaryFilePath();
        writeCachedNodesToFile(tempFile);

        swapFileWithTempFile(tempFile);
    }

    private void writeNodesToFile(Path file, Set<RemoteNode> nodes) throws IOException
    {
        BufferedWriter writer = Files.newBufferedWriter(file);

        writeNodes(writer, nodes);

        writer.flush();
        writer.close();
    }

    private void writeCachedNodesToFile(Path file) throws IOException
    {
        writeNodesToFile(file, cachedNodes);
    }

    public static void writeNodes
    (
        BufferedWriter writer,
        Set<RemoteNode> nodes
    )
        throws IOException
    {
        for(RemoteNode node : nodes)
        {
            HostPortPair pair = node.getHostPortPair();
            UUID id = node.getId();

            writer.write(pair.host + ":" + pair.port + ":" + id.toString());
            writer.newLine();
        }
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

        Files.move(tempFile, remoteNodeStoreFile, ATOMIC_MOVE);
    }

    private Path temporaryFilePath()
    {
        return remoteNodeStoreFile.getParent().resolve(uniqueTemporaryName());
    }

    private String uniqueTemporaryName()
    {
        return remoteNodeStoreFile.getFileName() + UUID.randomUUID().toString();
    }

    public static Stream<String> lineStreamFromString(String lines)
    {
        return Arrays.stream(lines.split("\n"));
    }

    public void updateNodes(String serializedNodes) throws IOException
    {
        updateCacheFromNodeStream
        (
            nodeStreamFromLineStream(lineStreamFromString(serializedNodes))
        );

        flushCachedNodesToDisk();
    }

    public Stream<RemoteNode> getNodesStream()
    {
        return cachedNodes.stream();
    }
}
