package org.josh.JoshDB.FileTrie;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author Josh Hight
 *
 * So the way MergeFile works is by writing furiously to the end of the file
 * and then building up the structure of the file when reading. So we need
 * a data structure that we can use to keep track of where all the various
 * chunks of object are.
 *
 * We have the guarantee that once something is written
 * to a MergeFile it will never change. We might write it out to a different
 * MergeFile, but it's place in the current MergeFile will never change.
 *
 * So what we need is a data stucture a bit like a list in that it can be
 * dynamically resized. But unlike a list, it need never shrink and writes to
 * a given offset will walways contain identical data.
 *
 * In the spirit of JoshDB we'll make it totally nonblocking, and the core
 * systemsy bits should be really efficient, but the mathy bits are kinda
 * whatever, we can always go back and optimize them if we ever feel like it
 * or if a profiler ever tells us that they're actually a problem.
 */
public class AtomicResizingLongArray
{
    private volatile long[][] masterList;
    // atomic updater for the master list, we use this instead of an
    // AtomicReference because we don't always need to access the masterList
    // atomically
    private static final AtomicReferenceFieldUpdater
    <
        AtomicResizingLongArray,
        long[][]
    >
    masterListUpdater =
        AtomicReferenceFieldUpdater.newUpdater
        (
            AtomicResizingLongArray.class,
            long[][].class,
            "masterList"
        );

    // OK, so last time I kind of fucking eyeballed the resizing and addressing.
    // That's not great, since the idea is to IMPROVE performance over the vanilla
    // implementation. How do we do this? By ensuring that the size/address
    // calculations can all be accomplished efficiently with constant time
    // shifting and masking. So how should we do this?
    // 000 -> 0,    1,
    // 001 -> 10,   11
    // 010 -> 100,  101,  110,  111
    // 111 -> 1000, 1001, 1010, 1011, 1100, 1101, 1110, 1111

    private static final int INT_BITS = Integer.BYTES * 8;

    /**
     * Given
     * @param subArrayNumber the index of a subArray in masterList
     * @return The length of that subArray
     */
    private int sizeOfSubArray(int subArrayNumber)
    {
        return (2 << subArrayNumber);
    }

    /**
     * Returns the highest set bit of an integer, from 0 to 31
     * Also returns 0 when passed 0
     * @param value
     * @return
     */
    private int highestSetBit(int value)
    {
        int highestBit = 0;
        // I really don't feel like unrolling this loop, is that something javac takes care of
        // or might we need a code generation step?
        for (int i = 0; i < INT_BITS; i++)
        {
            int mask = 1 << i;

            if ((mask & value) != 0)
            {
                highestBit = i;
            }
        }

        return highestBit;
    }

    /**
     *
     * @param offset 1 based offset (yes, you just add 1)
     * @return the shallow length required
     */
    private int shallowLengthRequiredForOffset(int offset)
    {
        return highestSetBit(offset) + 1;
    }


    private final AtomicInteger maxRequestedShallowLength;

    /**
     * Ensure that the deep length of masterList is at least enough to satisfy
     * a get/set at requiredOffset
     * @param requiredOffset The 1 based offset that we are going to address to
     *                      and therefore need to ensure has a home
     */
    void ensureMasterListLongEnough(int requiredOffset)
    {
        int requiredLength = shallowLengthRequiredForOffset(requiredOffset);

        if (masterList.length >= requiredLength) return;

        int localMax = maxRequestedShallowLength.get();

        boolean didSucceed = false;
        // set localMax to the deep Array Length we'll have after resizing
        while (localMax < requiredLength)
        {
            didSucceed = maxRequestedShallowLength.compareAndSet(localMax, requiredLength);

            if (didSucceed)
            {
                break;
            }
            else
            {
                localMax = maxRequestedShallowLength.get();
            }
        }

        //spin until the array has been resized by whoever succeeded
        if (!didSucceed)
        {
            //noinspection StatementWithEmptyBody
            while (masterList.length < requiredLength);
            return;
        }

        //spin until masterList has localMax subarrays allocated
        //noinspection StatementWithEmptyBody
        while (masterList.length < localMax);


        // then add our chunks
        // to do that we want to atomically swap out the master
        // list in case any other requests have the same required
        // length
        long[][] replacement = new long[requiredLength][];
        long[][] localMaster;
        do
        {
            localMaster = masterListUpdater.get(this);

            for (int i = 0; i < localMax; i++)
            {
                replacement[i] = localMaster[i];
            }

            for (int i = localMax; i < requiredLength; i++)
            {
                int sizeForArray = sizeOfSubArray(i);

                long[] subArrayReplacement = new long[sizeForArray];
                Arrays.fill(subArrayReplacement, -1);

                //replacement[i] = new long[sizeForArray];
                replacement[i] = subArrayReplacement;
            }
        }
        while
        (
            !masterListUpdater.compareAndSet(this, localMaster, replacement)
            &&
            localMaster.length < requiredLength
        );
    }

    public AtomicResizingLongArray()
    {
        masterList = new long[1][];
        masterList[0] = new long[2];
        maxRequestedShallowLength = new AtomicInteger(1);
    }

    public long get(int index)
    {
        int highestBit = highestSetBit(index);
        int subIndex = index == 0 ? 0 : index ^ highestBit;
        try
        {
            return masterList[highestBit][subIndex];
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            return -2;
        }
    }

    public void set(int index, long contents)
    {
        ensureMasterListLongEnough(index);
        int highestBit = highestSetBit(index);
        int subIndex = index == 0 ? 0 : index ^ highestBit;
        masterList[highestBit][subIndex] = contents;
    }
}
