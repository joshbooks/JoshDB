package org.josh.JoshDB.FileTrie;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsistencyTest
{
    private static final Path testLocus = Paths.get("./consistencyTestFile");
    private static final Path testBufferPath = Paths.get("./testBuffer");
    private static final byte[] testBuffer = new byte[324];

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

    static AtomicInteger threadNumber = new AtomicInteger(0);

    @Before
    public void zeroCounter()
    {
        threadNumber.set(0);
    }

    @After
    public void deleteTestFile()
    {
        try
        {
            Files.delete(testLocus);
        }
        catch (IOException e)
        {
            System.out.println("Got an IOException: " + e.getMessage());
            System.out.println("At: ");
            e.printStackTrace();
        }
    }

    static void writeThreadFunction()
    {
        int myThreadNumber = threadNumber.getAndIncrement();

        byte[] myBuffer = new byte[MergeFile.PIPE_BUF];

        Arrays.fill(myBuffer, (byte)myThreadNumber);

        try
        {
            MergeFile.mergeFileForPath(testLocus).appendToFileHelper(myBuffer);
        }
        catch (IOException e)
        {
            System.out.println("Got an IOException: " + e.getMessage());
            System.out.println("At: ");
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteConsistency() throws InterruptedException
    {
        int numThreads = 0x20;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++)
        {
            threads[i] = new Thread(ConsistencyTest::writeThreadFunction);
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++)
        {
            threads[i].join();
        }

        InputStream testReader;

        try
        {
            testReader = Files.newInputStream(testLocus);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return;
        }

        byte[] testBuffer = new byte[MergeFile.PIPE_BUF];
        boolean[] didFindBufferForThread = new boolean[numThreads];
        Arrays.fill(didFindBufferForThread, false);

        for (int i = 0; i < numThreads; i++)
        {
            try
            {
                int retVal = testReader.read(testBuffer, 0, testBuffer.length);
                if (retVal < MergeFile.PIPE_BUF)
                {
                    System.out.println("Failed to read a full buffer. WTF?");
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
                return;
            }

            byte valueForArray = testBuffer[0];
            boolean allCorrect = true;

            for (int j = 0; j < MergeFile.PIPE_BUF; j++)
            {
                if (testBuffer[j] != valueForArray)
                {
                    allCorrect = false;
                    break;
                }
            }

            if (allCorrect)
            {
                didFindBufferForThread[valueForArray] = true;
            }
            else
            {
                System.out.println("the " + valueForArray + "th array had at least one inconsistency");
            }
        }

        boolean missedAtLeastOne = false;
        for (int i = 0; i < numThreads; i++)
        {
            if (!didFindBufferForThread[i])
            {
                System.out.println("Didn't find " + i);
                missedAtLeastOne = true;
            }
        }

        assert !missedAtLeastOne;
    }

    @Test
    public void testSerializedObjectDelimiterLength()
    {
        // The size of a page minus the size of the delimiting magic numbers
        int usablePageSize = MergeFile.PIPE_BUF - (Integer.BYTES * 4);
        // account for the sequence number that gets embedded
        usablePageSize -= Long.BYTES;

        // Test a buffer that should perfectly fill one page
        byte[] serializedObject = new byte[usablePageSize];

        Arrays.fill(serializedObject, (byte) 13);

        assert
            MergeFile.numberOfPagesForSerializedObject(serializedObject.length)
            ==
            1;


        List<byte[]> delimitedObject =
            MergeFile
                .mergeFileForPath(testLocus)
                .delimitedObject(serializedObject);

        assert delimitedObject.size() == 1;


        // Test a buffer that's one byte too long to fit a page

        serializedObject = new byte[usablePageSize + 1];

        Arrays.fill(serializedObject, (byte) 13);

        assert
          MergeFile.numberOfPagesForSerializedObject(serializedObject.length)
          ==
          2;


        delimitedObject =
          MergeFile
            .mergeFileForPath(testLocus)
            .delimitedObject(serializedObject);

        assert delimitedObject.size() == 2;



    }

}
