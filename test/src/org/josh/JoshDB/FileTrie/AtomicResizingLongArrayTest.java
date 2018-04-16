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
}
