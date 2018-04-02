package org.josh.JoshDB.FileTrie;

import org.josh.JoshDb.FileTrie.MergeFile;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.SecureCacheResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class ConsistencyTest
{
    static Path testLocus = Paths.get("./consistencytestFile");

    SecureRandom entropy = SecureRandom.getInstanceStrong();

    public Runnable threadFunction =
    () ->
    {
        int objectLength = entropy.nextInt(MergeFile.PIPE_BUF * 5);

        byte[] object = new byte[objectLength];

        entropy.nextBytes(object);

        try
        {
            MergeFile.writeSerializedObject(testLocus, object);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        // todo test read once we can read out a
        // particular object instead of just the first one
    };

    public ConsistencyTest() throws NoSuchAlgorithmException {}

    @Test
    public void testReadWriteConsistency() throws InterruptedException
    {
        int numThreads = 5;
        Thread[] testThreads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++)
        {
            testThreads[i] = new Thread(threadFunction);
        }

        for (int i = 0; i < numThreads; i++)
        {
            testThreads[i].start();
        }

        for (int i = 0; i < numThreads; i++)
        {
            testThreads[i].join();
        }
    }
}
