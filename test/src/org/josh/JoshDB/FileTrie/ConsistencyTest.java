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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

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
        int numThreads = 0x7f;
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

        byte[] testBuffer;
        boolean[] didFindBufferForThread = new boolean[numThreads];
        Arrays.fill(didFindBufferForThread, false);

        for (int i = 0; i < numThreads; i++)
        {
            testBuffer =
              MergeFile
                .mergeFileForPath(testLocus)
                .nextPageRetNullOnError();

            byte valueForArray = testBuffer[0];

          System.out.println("value for array was " + valueForArray);

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

    // for use with testReadWriteObjectConsistency and associated methods
    private AtomicReferenceArray<Boolean> readSuccesses;

    private static final byte[] readWriteTestObject = new byte[MergeFile.PIPE_BUF * 7];
    static
    {
      Arrays.fill(readWriteTestObject, (byte) 69);
    }

    private void writeObjectThreadFunction()
    {
      // might want to compute these before starting threads to increase contention
      List<byte[]> delimitedPages =
        MergeFile
          .delimitedObject
          (
            readWriteTestObject,
            MergeFile.getObjectCount(testLocus).getAndIncrement()
          );

      for (byte[] page : delimitedPages)
      {
        try
        {
          MergeFile.mergeFileForPath(testLocus).appendToFileHelper(page);
        }
        catch (IOException e)
        {
          e.printStackTrace();
          assert false;
        }
      }
    }

    private void readObjectThreadFunction(int numThreads)
    {
      byte[] nextPage = null;

      HashMap<Long, List<byte[]>> sequenceNumberToPageList =
        new HashMap<>();

      while (true)
      {
        nextPage =
          MergeFile.mergeFileForPath(testLocus).nextPageRetNullOnError();

        if (nextPage == null)
        {
          break;
        }

        long sequenceNumberForPage =
          MergeFile.sequenceNumberOfPage(nextPage);
        if (sequenceNumberForPage > numThreads || sequenceNumberForPage < 0)
        {
          System.out.println("Got an invalid sequence number " + sequenceNumberForPage);
        }

        List<byte[]> pageList = new ArrayList<>();
        List<byte[]> temp =
          sequenceNumberToPageList.putIfAbsent(sequenceNumberForPage, pageList);

        if (temp != null)
        {
          pageList = temp;
        }

        pageList.add(nextPage);

      }

      for (int i = 0 ; i < numThreads; i++)
      {
        List<byte[]> pageList =
          sequenceNumberToPageList.get((long) i);

        byte[] object = MergeFile.undelimitedObject(pageList);

        if (object.length != readWriteTestObject.length)
        {
          System.out.println
          (
            "we got " + object.length + " bytes back out of "
            + readWriteTestObject.length + " total"
          );
        }

        if (Arrays.equals(object, readWriteTestObject))
        {
          // initialize value to true provided it doesn't exist yet

          while (readSuccesses.get(i) == null)
          {
            readSuccesses.compareAndSet(i, null, true);
          }
        }
        else
        {
          System.out.println("Encountered a mismatch for sequence number " + i);
          // set read success value to false
          readSuccesses.set(i, false);
        }
      }
    }

    void startAndJoinThreads(int numThreads, Runnable target)
    {
      Thread[] threads = new Thread[numThreads];

      for (int i = 0; i < numThreads; i++)
      {
        threads[i] = new Thread(target);
      }

      for (Thread thread: threads)
      {
        thread.start();
      }

      for (Thread thread: threads)
      {
        try
        {
          thread.join();
        }
        catch (InterruptedException e)
        {
          e.printStackTrace();
          assert false;
        }
      }
    }

    @Test
    public void testReadWriteObjectConsistency()
    {
      int numThreads = 0x20;

      readSuccesses = new AtomicReferenceArray<>(numThreads);

      startAndJoinThreads(numThreads, this::writeObjectThreadFunction);

      startAndJoinThreads(numThreads, () -> readObjectThreadFunction(numThreads));

      boolean globalSuccess = true;
      for (int i = 0; i < numThreads; i++)
      {
        if (!readSuccesses.get(i))
        {
          System.out.println("got a mismatch on " + i);
          globalSuccess = false;
        }
      }

      assert globalSuccess;
    }

    @Test
    public void testSequenceNumberParsing()
    {
        // bogus value for sequence number
        long sequenceNumber = 0x1337;

        // The size of a page minus the size of the delimiting magic numbers
        int usablePageSize = MergeFile.PIPE_BUF - (Integer.BYTES * 4);
        // account for the sequence number that gets embedded
        usablePageSize -= Long.BYTES;

        // Test a buffer that should perfectly fill one page
        byte[] serializedObject = new byte[usablePageSize];

        Arrays.fill(serializedObject, (byte) 13);


        List<byte[]> delimitedObject =
        MergeFile
            .delimitedObject(serializedObject, sequenceNumber);

        for (byte[] delimitedPage : delimitedObject)
        {
            assert MergeFile.isMemberOfObject(delimitedPage, sequenceNumber);
        }

        // Test a buffer that's one byte too long to fit a page
        serializedObject = new byte[usablePageSize + 1];

        Arrays.fill(serializedObject, (byte) 13);

        delimitedObject =
            MergeFile
            .delimitedObject(serializedObject, sequenceNumber);

        for (byte[] delimitedPage : delimitedObject)
        {
            assert MergeFile.isMemberOfObject(delimitedPage, sequenceNumber);
        }
    }

    @Test
    public void testSerializedObjectDelimiterLength()
    {
        // bogus value for sequence number
        long sequenceNumber = 0x1337;

        // The size of a page minus the size of the delimiting magic numbers
        int usablePageSize = MergeFile.PIPE_BUF - (Integer.BYTES * 4);
        // account for the sequence number that gets embedded
        usablePageSize -= Long.BYTES;
        usablePageSize -= Integer.BYTES;

        // Test a buffer that should perfectly fill one page
        byte[] serializedObject = new byte[usablePageSize];

        Arrays.fill(serializedObject, (byte) 13);

        assert
            MergeFile.numberOfPagesForSerializedObject(serializedObject.length)
            ==
            1;


        List<byte[]> delimitedObject =
            MergeFile
                .delimitedObject(serializedObject, sequenceNumber);

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
            .delimitedObject(serializedObject, sequenceNumber);

        assert delimitedObject.size() == 2;
    }

  @Test
  public void testDelimitingUndelimitingConsistency()
  {
    // The size of a page minus the size of the delimiting magic numbers
    int usablePageSize = MergeFile.PIPE_BUF - (Integer.BYTES * 4);
    // account for the sequence number that gets embedded
    usablePageSize -= Long.BYTES;
    usablePageSize -= Integer.BYTES;
    testDelimitingUndelimitingConsistencyForLength(usablePageSize);

    // odd numbers, dumb programmers never see that coming,
    // bet whatever stupid dev wrote this screwed that up
    testDelimitingUndelimitingConsistencyForLength((usablePageSize + 5) + usablePageSize * 3);
  }

  public void testDelimitingUndelimitingConsistencyForLength(int length)
  {
    byte[] object = new byte[length];

    Arrays.fill(object, (byte)13);

    // make sure we're dealing with longs properly
    long sequenceNumber = Long.MAX_VALUE;

    List<byte[]> delimitedPages = MergeFile.delimitedObject(object, sequenceNumber);

    for (byte[] delimitedPage: delimitedPages)
    {
      assert MergeFile.sequenceNumberOfPage(delimitedPage) == sequenceNumber;
      assert delimitedPage.length == MergeFile.PIPE_BUF;
    }

    byte[] reUnDelimited = MergeFile.undelimitedObject(delimitedPages);

    assert reUnDelimited.length == object.length;

    assert Arrays.equals(reUnDelimited, object);
  }

}
