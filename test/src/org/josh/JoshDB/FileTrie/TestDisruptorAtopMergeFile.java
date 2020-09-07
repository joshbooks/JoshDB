package org.josh.JoshDB.FileTrie;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

@NotThreadSafe
public class TestDisruptorAtopMergeFile
{
  private static final Path testLocus = Paths.get("./disruptorMergeTestFile");
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

  private static final MergeFile testMergeFile =
    MergeFile.mergeFileForPath(testLocus);


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
      super.setPriority(0);
    }
  }

  // So in order to use single producer single consumer disruptors
  // I think the ideal paradigm would be to have set of threads
  // (
  //    1 server thread,
  //    1 disk writer thread,
  //    1 compute/in memory object updater thread)
  // )
  // all linked up with one disruptor
  // and if we need to increase capacity we just instantiate another one of those

  private static class TestThreadFactory implements ThreadFactory
  {
    private final ThreadFactory inner;

    TestThreadFactory(ThreadFactory wrappee)
    {
       this.inner = wrappee;
    }

    TestThreadFactory()
    {
      this
      (
        r
        ->
        {
          Thread thread = Executors.defaultThreadFactory().newThread(r);

          thread.setPriority(Thread.MAX_PRIORITY);

          return thread;
        }
      );
    }

    @Override
    public Thread newThread(Runnable r)
    {
      Thread thread = inner.newThread(r);

      thread.setPriority(Thread.MAX_PRIORITY);

      return thread;
    }
  }

  private Disruptor<byte[]> setupDisruptor()
  {
    Disruptor<byte[]> disruptor =
      new Disruptor
      <>
      (
        () ->
        new byte[MergeFile.PIPE_BUF],
        0x200,
        new TestThreadFactory(),
        ProducerType.SINGLE,
        new BlockingWaitStrategy()
      )
    ;

    //noinspection unchecked
    disruptor
      .handleEventsWith
      (
        (buffer, sequenceNum, endOfBatch) ->
        testMergeFile.appendToFileHelper(buffer)
      )
    ;

    return disruptor;
  }
//
//  @Test
//  public void testSingleDisruptorWrites()
//  {
//    testDisruptorWrites(1, 2048);
//  }
//
//  @Test
//  public void testFourDisruptorWrites()
//  {
//    testDisruptorWrites(4, 512);
//  }

  @Test
  public void testEightDisruptorWrites()
  {
    testDisruptorWrites(8, 256);
  }
//
//  @Test
//  public void testSixteenDisruptorWrites()
//  {
//    testDisruptorWrites(16, 128);
//  }

  @Test
  public void testThirtyTwoDisruptorWrites()
  {
    testDisruptorWrites(32, 64);
  }

  @Test
  public void testSixtyFourDisruptorWrites()
  {
    testDisruptorWrites(64, 32);
  }

  private void testDisruptorWrites(int numThreads, int numIterationsPerThread)
  {
    // so this will get better once we have vectorized writes
    // but here we go
    // so we implement a disruptor

    Random random = new Random();
    byte[] serializedObject = new byte[8192];
    random.nextBytes(serializedObject);
    long numPagesForObject =
      MergeFile.numberOfPagesForSerializedObject(serializedObject.length);

    List<Disruptor<byte[]>> disruptors = new ArrayList<>();


    for (int i = 0; i < numThreads; i++)
    {
      disruptors.add(setupDisruptor());
    }

    final AtomicInteger doneCount = new AtomicInteger(0);

    Thread[] threads = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++)
    {
      threads[i] =
        new Thread
        (
          () ->
          {
            int myThreadNum = threadNumber.getAndIncrement();
            for (int j = 0; j < (long) numIterationsPerThread; j++)
            {
              List<byte[]> delimitedPages =
              MergeFile
                .delimitedObject
                (
                  serializedObject,
                  j + (myThreadNum * (long) numIterationsPerThread)
                )
              ;

              for (byte[] page : delimitedPages)
              {
                disruptors.get(myThreadNum)
                  .publishEvent
                  (
                    (event, sequence) ->
                    System.arraycopy(page, 0, event, 0, page.length)
                  )
                ;
              }
            }

            doneCount.incrementAndGet();
          }
        )
      ;
    }

    long beginTime = System.nanoTime();

    for (int i = 0; i < numThreads; i++)
    {
      disruptors.get(i).start();
    }

    for (Thread thread : threads)
    {
      thread.start();
    }

    for (Thread thread : threads)
    {
      try
      {
        thread.join();
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
    }

    for (int i = 0; i < numThreads; i++)
    {
      disruptors.get(i).shutdown();
    }

    long endTime = System.nanoTime();

    System
      .out
      .println
      (
        "It took " + (endTime - beginTime) + " nanoseconds to write "
        + ((long) numIterationsPerThread * numThreads * numPagesForObject)
        + " pages with " + disruptors.size()
        + " disruptor(s) with ringbuffer size "
        + disruptors.get(0).getBufferSize()
      );
  }


}
