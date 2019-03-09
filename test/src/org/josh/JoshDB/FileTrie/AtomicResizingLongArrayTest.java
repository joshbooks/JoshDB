package org.josh.JoshDB.FileTrie;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class AtomicResizingLongArrayTest
{

    @Test
    public void testSizeIncrementation()
    {
        AtomicResizingLongArray incrementingThing = new AtomicResizingLongArray();

        assert incrementingThing.currentLengthEstimate() == 0;

        incrementingThing.set(2, 5);

        System.out.println("length is now " + incrementingThing.currentLengthEstimate());

        assert incrementingThing.currentLengthEstimate() == 8;
    }

    @Test
    public void testValuePreservation()
    {
        AtomicResizingLongArray testThing = new AtomicResizingLongArray();

        for (int i = 1; i < 1337; i++)
        {
            testThing.set(i - 1, i);
        }

        for (int i = 1; i < 1337; i++)
        {
            assert testThing.get(i - 1) == i;
        }
    }

    @Test
    public void testAddUp()
    {
        AtomicResizingLongArray array = new AtomicResizingLongArray();

        for (int i = 0; i < 0x300; i += 8)
        {
            array.set(i, ByteBuffer.allocate(8).putLong(i).array());
            if (array.get(i/8) != i)
            {
                System.out.println(i/8 + " was not equal to " + array.get(i/8));
            }
        }

        for (int i = 0; i < 0x300; i += 8)
        {
            assert ByteBuffer.wrap(array.get(i, 8)).getLong() == i;
        }
    }

    @Test
    public void testAddUpMultiThreaded() throws InterruptedException
    {
        AtomicResizingLongArray array = new AtomicResizingLongArray();
        Thread[] testThreads = new Thread[0x200];
        AtomicInteger threadNum = new AtomicInteger(0);

        for (int i = 0; i < 0x200; i += 8)
        {
//            final int finalI = i;
            testThreads[i] =
                new Thread
                (
                    () ->
                    {
                        int finalI = threadNum.getAndAdd(8);
                        System.out.println("thread " + Thread.currentThread().getName() + " got " + finalI);
                        for (int j = 0; j < 0x100; j++)
                        {
                            int currentValue = ( finalI * 0x100 ) + j;
                            array.set(currentValue * 8, ByteBuffer.allocate(8).putLong(finalI).array());
                        }
                    }
                );

            testThreads[i].start();
        }

        for (int i = 0; i < 0x200; i += 8)
        {
            testThreads[i].join();
        }

        for (int i = 0; i < 0x200; i += 8)
        {
            System.out.println(i);
            for (int j = 0; j < 0x100; j++)
            {
                int currentValue = (( i * 0x100 ) + j) * 8;
                if (ByteBuffer.wrap(array.get(currentValue, 8)).getLong() != i)
                {
                    System.out.println(i + " was not equal to " + ByteBuffer.wrap(array.get(currentValue, 8)).getLong());
                }
//                assert ByteBuffer.wrap(array.get(currrentValue, 8)).getLong() == i;
            }
        }
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
