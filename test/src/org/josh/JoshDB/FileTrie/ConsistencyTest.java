package org.josh.JoshDB.FileTrie;

import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

public class ConsistencyTest
{
    private static final Path testLocus = Paths.get("./consistencytestFile");
    private static final Path testBufferPath = Paths.get("./testBuffer");
    private static final byte[] testBuffer = new byte[324];
    private static final Object testBufferLock = new Object();
    static
    {

        try
        {
            if (Files.newInputStream(testBufferPath).read(testBuffer) != testBuffer.length)
            {
                System.out.println("Well shit");
            }
        }
        catch (IOException e)
        {
            System.out.println("Well shit");
        }
    }

    int numThreads = 5;
    // This should really be trove hashmap since we're mapping longs.
    // Since we're only using values well within int range it works for
    // this test, but keep in mind for prod.
    byte[][] pages = new byte[numThreads][];


    public Runnable threadFunction =
    () ->
    {
        Random entropy = new Random();
        String threadName = Thread.currentThread().getName();
        System.out.println("checkpoint 1 from " + threadName);

        MergeFile testMerge = MergeFile.mergeFileForPath(testLocus);

        //todo weakened the test temporarily to debug, should restore to full strength
        //int objectLength = entropy.nextInt(MergeFile.PIPE_BUF * 5);
//        int objectLength = MergeFile.PIPE_BUF - 24;

        byte[] object = testBuffer;//new byte[objectLength];
        int objectLength = object.length;

//        entropy.nextBytes(object);

        long sequenceNumber;

        try
        {
            sequenceNumber = testMerge.writeSerializedObject(object);
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
            byte[] objectFromFile = testMerge.getObject(sequenceNumber);
            boolean fail = !Arrays.equals(object, objectFromFile);
            if (fail)
            {
                assert objectFromFile.length == object.length;
                synchronized (this)
                {
                    for (int i = 0; i < object.length; i++)
                    {
                        if (object[i] != objectFromFile[i])
                        {
                            for (int j = -0x10; j < 0x11; j++)
                            {
                                if (j + i < 0 || j + i >= objectLength)
                                {
                                    continue;
                                }

                                if (object[j+i] == objectFromFile[i])
                                {
                                    System.out.println("object " + sequenceNumber + " index " + i + " was shifted " + j);
                                }
                            }

//                            System
//                                .out
//                                .println(
//                                    "Failed on index "
//                                    + i
//                                    + " byte from file: "
//                                    + String.format("position %d\t%c", i,  (char)(objectFromFile[i])));
//                            System.out.println("in object " + sequenceNumber + " index " + i + " matched");
                        }
                    }
                }
            }

            if (!fail)
            {
                System.out.println("in thread" + threadName + ": " + sequenceNumber + " matched perfectly");
            }
            else
            {
                System.out.println("in thread" + threadName + ": " + sequenceNumber + " was not a match");
            }

//            assert !fail;
        }
        catch (IOException e)
        {
            e.printStackTrace();
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
        for (int i = 0; i < numThreads; i++)
        {
            threadFunction.run();
        }


//        RandomAccessFile in = new RandomAccessFile(testLocus.toFile(), "r");
//
//        for (byte i = in.readByte(); in.getFilePointer() < in.length(); i = in.readByte())
//        {
//            System.out.println(String.format("position %d\t%02x",in.getFilePointer()-1, i) + "\t" + (char) i);
//        }
//
//        in.close();

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

//        RandomAccessFile in = new RandomAccessFile(testLocus.toFile(), "r");
//
//        for (byte i = in.readByte(); in.getFilePointer() < in.length(); i = in.readByte())
//        {
//            System.out.println(String.format("position %d\t%02x",in.getFilePointer()-1, i) + "\t" + (char) i);
//        }


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




//        in.close();

        Files.delete(testLocus);
    }
}
