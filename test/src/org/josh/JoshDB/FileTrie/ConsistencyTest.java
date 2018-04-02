package org.josh.JoshDB.FileTrie;

import org.josh.JoshDb.FileTrie.MergeFile;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.SecureCacheResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

public class ConsistencyTest
{
    static Path testLocus = Paths.get("./consistencytestFile");

    public Runnable threadFunction =
    () ->
    {
        Random entropy = new Random();
        System.out.println("checkpoint 1 from "+Thread.currentThread().getName());

        int objectLength = entropy.nextInt(MergeFile.PIPE_BUF * 5);

        System.out.println("checkpoint 2 from "+Thread.currentThread().getName());
        byte[] object = new byte[objectLength];
        System.out.println("checkpoint 3 from "+Thread.currentThread().getName());

        entropy.nextBytes(object);

        System.out.println("checkpoint 4 from "+Thread.currentThread().getName());


        try
        {
            System.out.println("writing");
            MergeFile.writeSerializedObject(testLocus, object);
            System.out.println("written");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        // todo test read once we can read out a
        // particular object instead of just the first one
    };

    public ConsistencyTest() throws NoSuchAlgorithmException
    {
    }

    @Test
    public void testReadWriteConsistency() throws InterruptedException, IOException
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

        RandomAccessFile in = new RandomAccessFile(testLocus.toFile(), "r");

        for (byte i = in.readByte(); in.getFilePointer() < in.length(); i = in.readByte())
        {
            System.out.println(String.format("%02x", i) + "\t" + (char) i);
        }

        in.close();

        Files.delete(testLocus);
    }
}
