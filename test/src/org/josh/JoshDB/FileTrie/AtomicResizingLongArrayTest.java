package org.josh.JoshDB.FileTrie;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class AtomicResizingLongArrayTest
{
    @Test
    public void testValuePreservation()
    {
        AtomicResizingLongArray testThing = new AtomicResizingLongArray();

        for (int i = 0; i < 1337; i++)
        {
            testThing.set(i, (i * (Long.MAX_VALUE >> 4)));
        }

        for (int i = 0; i < 1337; i++)
        {
            assert testThing.get(i) == (i * (Long.MAX_VALUE >> 4));
        }
    }


    /**
     * This test serves no real, purpose, you may notice it has no asserts,
     * bu it does a fantastic job of boosting my ego and demonstrating that
     * if you grow aggressively you can write a thread safe collectiony thing
     * that's faster than an ArrayList for some workloads
     */
    @Test
    public void testRelativeSpeed()
    {
        long myStartTime = System.nanoTime();

        AtomicResizingLongArray testThing = new AtomicResizingLongArray();

        for (int i = 0; i < 0x1000000; i++)
        {
            testThing.set(i, (i * (Long.MAX_VALUE >> 4)));
        }

        for (int i = 0; i < 0x1000000; i++)
        {
            if (testThing.get(i) != (i * (Long.MAX_VALUE >> 4)))
            {
                System.out.println("Failed on " + i);
                assert false;
            }
        }

        long myEndTime = System.nanoTime();
        long myTime = myEndTime - myStartTime;

        long libStartTime = System.nanoTime();
        ArrayList<Long> libTestThing = new ArrayList<>();
        for (int i = 0; i < 0x1000000; i++)
        {
            libTestThing.ensureCapacity(i - 1);
            while (libTestThing.size() <= i)
            {
                libTestThing.add(0L);
            }
            libTestThing.set(i, (i * (Long.MAX_VALUE >> 4)));
        }

        for (int i = 0; i < 0x1000000; i++)
        {
            assert libTestThing.get(i) == (i * (Long.MAX_VALUE >> 4));
        }
        long libEndTime = System.nanoTime();
        long libTime = libEndTime - libStartTime;

        System.out.println("The time for my library was                " + myTime);
        System.out.println("The time for the standard java library was " + libTime);
    }

    @Test
    public void testAddUpLongsMultiThreaded() throws InterruptedException
    {
        AtomicResizingLongArray array = new AtomicResizingLongArray();
        Thread[] testThreads = new Thread[0x100];

        for (int i = 0; i < 0x100; i += 8)
        {
            final int finalI = i;
            testThreads[i] =
                    new Thread
                    (
                        () ->
                        {
                            array.set(finalI, finalI);
                        }
                    );

            testThreads[i].start();
        }

        for (int i = 0; i < 0x100; i += 8)
        {
            testThreads[i].join();
        }

        for (int i = 0; i < 0x100; i += 8)
        {

            assert array.get(i) == i;
        }
    }
}
