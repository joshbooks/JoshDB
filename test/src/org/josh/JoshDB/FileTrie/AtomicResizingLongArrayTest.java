package org.josh.JoshDB.FileTrie;

import org.josh.JoshDb.FileTrie.AtomicResizingLongArray;
import org.junit.Test;

import java.nio.ByteBuffer;

public class AtomicResizingLongArrayTest
{
    @Test
    public void testAddUp() throws InterruptedException
    {
        AtomicResizingLongArray array = new AtomicResizingLongArray();

        for (int i = 0; i < 0x100; i += 8)
        {
            array.set(i, ByteBuffer.allocate(8).putLong(i).array());
        }

        for (int i = 0; i < 0x100; i += 8)
        {
            assert ByteBuffer.wrap(array.get(i, 8)).getLong() == i;
        }
    }

    @Test
    public void testAddUpMultiThreaded() throws InterruptedException
    {
        AtomicResizingLongArray array = new AtomicResizingLongArray();
        Thread[] testThreads = new Thread[0x100];

        for (int i = 0; i < 0x100; i += 8)
        {
            final int finalI = i;
            testThreads[i] =
            new Thread(() ->
            {
                try
                {
                    array.set(finalI, ByteBuffer.allocate(8).putLong(finalI).array());
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
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

            assert ByteBuffer.wrap(array.get(i, 8)).getLong() == i;
        }
    }
}
