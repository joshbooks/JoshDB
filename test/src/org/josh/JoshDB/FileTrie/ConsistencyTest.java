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
import java.util.Arrays;
import java.util.Random;

public class ConsistencyTest
{
    static Path testLocus = Paths.get("./consistencytestFile");

    int numThreads = 5;
    byte[][] pages = new byte[numThreads][];


    public Runnable threadFunction =
    () ->
    {
        Random entropy = new Random();
        System.out.println("checkpoint 1 from "+Thread.currentThread().getName());

        //todo weakened the test temporarily to debug, should restore to full strength
        //int objectLength = entropy.nextInt(MergeFile.PIPE_BUF * 5);
        int objectLength = MergeFile.PIPE_BUF - 24;

        byte[] object = new byte[objectLength];

        entropy.nextBytes(object);

        long sequenceNumber = -1;

        try
        {
            sequenceNumber = MergeFile.writeSerializedObject(testLocus, object);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return;
        }

        pages[(int)sequenceNumber] = object;

        // todo test read once we can read out a
        // particular object instead of just the first one

        try
        {
            byte[] objectFromFile = MergeFile.mergeFileForPath(testLocus).getObject(sequenceNumber);
            boolean fail = !Arrays.equals(object, objectFromFile);
            if (fail)
            {
                assert objectFromFile.length == object.length;
                synchronized (this)
                {
                    for (int i = 0; i < object.length; i++)
                    {
                        if (object[i] == objectFromFile[i])
                        {
                            System.out.println("in object " + sequenceNumber + " index " + i + " matched");
                        }
                    }
                }
            }
            assert !fail;

            System.out.println(sequenceNumber+" matched perfectly");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return;
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    };

    public ConsistencyTest() throws NoSuchAlgorithmException
    {
    }

    @Test
    public void testSingleThreadedConsistency() throws IOException
    {
        for (int i = 0; i < 5; i++)
        {
            threadFunction.run();
        }


        RandomAccessFile in = new RandomAccessFile(testLocus.toFile(), "r");

        for (byte i = in.readByte(); in.getFilePointer() < in.length(); i = in.readByte())
        {
            System.out.println(String.format("position %d\t%02x",in.getFilePointer()-1, i) + "\t" + (char) i);
        }

        in.close();

        Files.delete(testLocus);
    }

    @Test
    public void testReadWriteConsistency() throws InterruptedException, IOException
    {
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
            System.out.println(String.format("position %d\t%02x",in.getFilePointer()-1, i) + "\t" + (char) i);
        }


        //in = new RandomAccessFile(testLocus.toFile(), "r");

        for (int i = 0; i < numThreads; i++)
        {
            byte[] singleThreadedFileRead = MergeFile.mergeFileForPath(testLocus).getObject(i);

            boolean fail = !Arrays.equals(singleThreadedFileRead, pages[i]);

            //todo someday this should be an assert
            if (fail)
            {
                System.out.println("Object with sequence number "+ i+ " was still inconsistent when we read it single threaded");
            }
        }




        in.close();

        Files.delete(testLocus);
    }
}
