package org.josh.JoshDB.FileTrie;

import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

@NotThreadSafe
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

    @After
    public void invalidateTestFileMetadata()
    {
      MergeFile.mergeFileForPath(testLocus).sequenceNumberToPageInfoList.clear();
      MergeFile.mergeFileForPath(testLocus).byteChannelForThread = new ThreadLocal<>();
    }

    static class TestThread extends Thread
    {
      static class TestRunnable implements Runnable
      {
        Runnable inner;


        public TestRunnable(Runnable r)
        {
          this.inner = r;
        }

        @Override
        public void run()
        {
          inner.run();
          try
          {
            MergeFile.mergeFileForPath(testLocus).closeAppender();
            MergeFile.mergeFileForPath(testLocus).closeByteChannel();
          }
          catch (IOException e)
          {
            System.out.println("Got an exception closing up: " + e.getMessage());
          }
        }
      }

      public TestThread(Runnable r)
      {
        this(new TestRunnable(r));
      }

      public TestThread(TestRunnable r)
      {
        super(r);
      }
    }

    private static void writeThreadFunction()
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
        TestThread[] threads = new TestThread[numThreads];
        for (int i = 0; i < numThreads; i++)
        {
            threads[i] = new TestThread(ConsistencyTest::writeThreadFunction);
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

    private static final byte[] readWriteTestObject = new byte[MergeFile.PIPE_BUF * 21];
    static
    {
      Arrays.fill(readWriteTestObject, (byte) 69);
    }
    private static final List<List<byte[]>> delimitedReadWritePagesList = new ArrayList<>();

    private static final int numThreads = 0x20;
    static
    {
      for (int i = 0; i < numThreads; i++)
      {
        delimitedReadWritePagesList
          .add
          (
            MergeFile.delimitedObject(readWriteTestObject, (long) i)
          );
      }
    }

    private void writeObjectThreadFunction()
    {
      long objectSequenceNumber =
        MergeFile.getObjectCount(testLocus).getAndIncrement();

      List<byte[]> delimitedPages =
        delimitedReadWritePagesList.get((int) objectSequenceNumber);

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

    private void readObjectThreadFunction()
    {
      byte[] nextPage;

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
        if (sequenceNumberForPage > ConsistencyTest.numThreads || sequenceNumberForPage < 0)
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

      for (int i = 0; i < ConsistencyTest.numThreads; i++)
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
      TestThread[] threads = new TestThread[numThreads];

      for (int i = 0; i < numThreads; i++)
      {
        threads[i] = new TestThread(target);
      }

      for (TestThread thread: threads)
      {
        thread.start();
      }

      for (TestThread thread: threads)
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
      readSuccesses = new AtomicReferenceArray<>(numThreads);

      startAndJoinThreads(numThreads, this::writeObjectThreadFunction);

      startAndJoinThreads(numThreads, this::readObjectThreadFunction);

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

    // wow, single threaded is sooooo sloooooooowwww I thought the test was broken for a while
    @Test
    public void testSingleThreadedMetadataConsistency() throws IOException
    {
      MergeFile testMergeFile = MergeFile.mergeFileForPath(testLocus);

      boolean globalDidFail = false;
      for (int i = 0; i < numThreads; i++)
      {
        List<byte[]> delimitedObject = MergeFile.delimitedObject(readWriteTestObject, (long) i);

        assert delimitedObject.size() > 1;


        testMergeFile.appendToFileHelper(delimitedObject.get(0));
        testMergeFile.nextPageRetNullOnError();


        // after writing the first page the metadata should say the object
        // is incomplete and have exactly one entry in the page list
        AtomicReference<MergeFile.PersistedObjectInfo> atomicObjectInfo =
          testMergeFile.sequenceNumberToPageInfoList.get((long) i);

        assert atomicObjectInfo != null;

        MergeFile.PersistedObjectInfo objectInfo = atomicObjectInfo.get();

        assert objectInfo != null;

        assert !objectInfo.isPageArrComplete();

        assert objectInfo.pageInfoArr.length == 1;

        for (int j = 1; j < delimitedObject.size(); j++)
        {
          testMergeFile.appendToFileHelper(delimitedObject.get(j));
          testMergeFile.nextPageRetNullOnError();
        }

        // after writing all pages for an object the metadata should say that the
        // object is complete and have all entries is the page list

        atomicObjectInfo =
          testMergeFile.sequenceNumberToPageInfoList.get((long) i);

        assert atomicObjectInfo != null;

        objectInfo = atomicObjectInfo.get();

        boolean didFail = false;

        if (objectInfo == null)
        {
          System.out.println("objectInfo was null for " + i + ", that's fucked");
          didFail = true;
        }

        if (!didFail && !objectInfo.isPageArrComplete())
        {
          System.out.println("Object info wasn't complete for " + i+ "after writing all the pages");
          System.out.println("so we wrote out " + delimitedObject.size() + " pages, but only " + objectInfo.pageInfoArr.length + " wound up in metadata");
          didFail = true;
        }

        if (!didFail && objectInfo.pageInfoArr.length != delimitedObject.size())
        {
          System.out.println("so we wrote out " + delimitedObject.size() + " pages, but only " + objectInfo.pageInfoArr.length + " wound up in metadata");
          didFail = true;
        }

        globalDidFail |= didFail;
      }

      assert !globalDidFail;
    }

    @Test
    public void testReadWriteMetadataConsistency()
    {
      byte[][] testArray = new byte[numThreads][];
      // todo better source of entropy
      Random random = new Random();

      for (int i = 0; i < testArray.length; i++)
      {
        testArray[i] = new byte[random.nextInt(0x10000)];
        random.nextBytes(testArray[i]);
      }

      List<List<byte[]>> delimitedPageLists = new ArrayList<>(testArray.length);

      for (int i = 0; i < testArray.length; i++)
      {
        delimitedPageLists.add(i, MergeFile.delimitedObject(testArray[i], i));
      }


      for (int i = 0; i < delimitedPageLists.size(); i++)
      {
        int sizeOfData =
          MergeFile
            .amountRemainingForPage
            (
              delimitedPageLists.get(i).get(0)
            );

        assert sizeOfData == testArray[i].length;

        assert
          Arrays
            .equals
            (
              testArray[i],
              MergeFile.undelimitedObject(delimitedPageLists.get(i))
            );
      }

      AtomicInteger threadNumberTracker = new AtomicInteger(0);

      startAndJoinThreads
      (
        testArray.length,
        () ->
        {
          int threadNum = threadNumberTracker.getAndIncrement();
          for (byte[] page : delimitedPageLists.get(threadNum))
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
      );

      threadNumberTracker.set(0);

      final boolean[] readSucesses = new boolean[testArray.length];

      // I seem to recall boolean arrays getting inited to false,
      // but let's make sure
      for (int i = 0; i < readSucesses.length; i++)
      {
        readSucesses[i] = false;
      }

      startAndJoinThreads
      (
        testArray.length,
        () ->
        {
          int threadNum = threadNumberTracker.getAndIncrement();
          try
          {
            byte[] expected = testArray[threadNum];
            byte[] actual =
              MergeFile.mergeFileForPath(testLocus).getObject(threadNum);

            if (expected == null || actual == null)
            {
              readSucesses[threadNum] = false;
            }

            if (expected ==  null)
            {
              System.out.println("Expected value was null, that's super weird");
              Thread.dumpStack();
              return;
            }

            if (actual == null)
            {
              System.out.println("actual value was null for "+ threadNum +", that's not good at all");
              Thread.dumpStack();
              return;
            }

            int expectedLength = expected.length;
            int actualLength = actual.length;

            if (expectedLength != actualLength)
            {
              System.out.println("length mismatch on " + threadNum);
              readSucesses[threadNum] = false;
              return;
            }
            if
            (
              !
              Arrays
                .equals
                (
                  testArray[threadNum],
                  actual
                )
            )
            {
              System.out.println("Got a mismatch for " + threadNum);
              readSucesses[threadNum] = false;
            }
          }
          catch (IOException e)
          {
            System.out.println("This is really not good");
            System.out.println("Got a mismatch with object number " + threadNum);
            e.printStackTrace();
            readSucesses[threadNum] = false;
          }

          readSucesses[threadNum] = true;
        }
      );

      boolean globalSuccess = true;
      for (int i = 0; i < readSucesses.length; i++)
      {
        if (!readSucesses[i])
        {
          globalSuccess = false;
          System.out.println("Read failed on " + i);
        }
      }

      assert globalSuccess;
    }

  @Test
  public void testReadBackwardWriteForwardMetadataConsistency()
  {
    byte[][] testArray = new byte[numThreads][];
    // todo better source of entropy
    Random random = new Random();

    for (int i = 0; i < testArray.length; i++)
    {
      testArray[i] = new byte[random.nextInt(0x10000)];
      random.nextBytes(testArray[i]);
    }

    List<List<byte[]>> delimitedPageLists = new ArrayList<>(testArray.length);

    for (int i = 0; i < testArray.length; i++)
    {
      delimitedPageLists.add(i, MergeFile.delimitedObject(testArray[i], i));
    }

    for (int i = 0; i < delimitedPageLists.size(); i++)
    {
      assert
        Arrays
          .equals
          (
            testArray[i],
            MergeFile.undelimitedObject(delimitedPageLists.get(i))
          );
    }

    AtomicInteger threadNumberTracker = new AtomicInteger(0);

    startAndJoinThreads
    (
      testArray.length,
      () ->
      {
        int threadNum = threadNumberTracker.getAndIncrement();
        for (byte[] page : delimitedPageLists.get(threadNum))
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
    );

    threadNumberTracker.set(testArray.length);

    final boolean[] readSucesses = new boolean[testArray.length];

    // I seem to recall boolean arrays getting inited to false,
    // but let's make sure
    for (int i = 0; i < readSucesses.length; i++)
    {
      readSucesses[i] = false;
    }

    // todo use readSuccesses here so we can assert false in the main thread
    startAndJoinThreads
    (
      testArray.length,
      () ->
      {
        int threadNum = threadNumberTracker.decrementAndGet();
        try
        {
          byte[] expected = testArray[threadNum];
          byte[] actual =
            MergeFile.mergeFileForPath(testLocus).getObject(threadNum);

          if (expected == null || actual == null)
          {
            readSucesses[threadNum] = false;
          }

          if (expected ==  null)
          {
            System.out.println("Expected value was null, that's super weird");
            Thread.dumpStack();
            return;
          }

          if (actual == null)
          {
            System.out.println("actual value was null for "+ threadNum +", that's not good at all");
            Thread.dumpStack();
            return;
          }

          int expectedLength = expected.length;
          int actualLength = actual.length;

          if (expectedLength != actualLength)
          {
            System.out.println("length mismatch on " + threadNum);
            readSucesses[threadNum] = false;
            return;
          }
          if
          (
            !
            Arrays
              .equals
              (
                testArray[threadNum],
                actual
              )
          )
          {
            System.out.println("Got a mismatch for " + threadNum);
            readSucesses[threadNum] = false;
          }
        }
        catch (IOException e)
        {
          System.out.println("This is really not good");
          System.out.println("Got a mismatch with object number " + threadNum);
          e.printStackTrace();
          readSucesses[threadNum] = false;
        }

        readSucesses[threadNum] = true;
      }
    );

    boolean globalSuccess = true;
    for (int i = 0; i < readSucesses.length; i++)
    {
      if (!readSucesses[i])
      {
        globalSuccess = false;
        System.out.println("Read failed on " + i);
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
