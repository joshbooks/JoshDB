package org.josh.JoshDB;

import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class TestRemoteNodeStore
{
    @Test
    public void testReadWriteConsistency() throws IOException
    {
        RemoteNodeStore store = new RemoteNodeStore();

        Set<RemoteNode> testRemoteSet = new HashSet<>();

        for (int i = 0 ; i < 10; i++)
        {
            testRemoteSet.add(new RemoteNode("test" + i, i, new UUID(i, i)));
        }

        store.updateCacheFromNodeStream(testRemoteSet.stream());

        store.flushCachedNodesToDisk();

        store.updateCacheFromNodeStore();

        Set<RemoteNode> returnedNodes =
            store.getNodesStream().collect(Collectors.toSet());

        returnedNodes.forEach(returned -> {assert testRemoteSet.contains(returned);});

        testRemoteSet.forEach(test -> {assert returnedNodes.contains(test);});

        Files.delete(store.storePath());
    }
}
