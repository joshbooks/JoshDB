package org.josh.JoshDb.FileTrie;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class AtomicResizingLongArray
{

    //64*2/8 = 128/8 =
    private static final int ELEMENT_LENGTH = 8;

    //an array of byte[]s where masterList[i].length == ELEMENT_LENGTH << i
    private volatile byte[][] masterList;
    private static final AtomicReferenceFieldUpdater
    <
        AtomicResizingLongArray,
        byte[][]
    >
    masterListUpdater =
        AtomicReferenceFieldUpdater.newUpdater
        (
            AtomicResizingLongArray.class,
            byte[][].class,
            "masterList"
        );

    private int masterListDeepLength(int shallowLength)
    {
        int deepLength = 0;
        for (int i = 0; i < shallowLength; i++)
        {
            deepLength += ELEMENT_LENGTH << (i - 1);
        }

        return deepLength;
    }

    private int shallowLengthRequiredForOffset(int offset)
    {
        int proposed;
        int accepted = 1;
        for (proposed = 1; masterListDeepLength(proposed) < offset; proposed++)
        {
            accepted = proposed;
        }

        return accepted;
    }

    private final AtomicInteger maxRequestedShallowLength;

    /**
     * Ensure that the deep length of masterList is
     * at least enough to satisfy a get at requiredOffset
     */
    void ensureMasterListLongEnough(int requiredOffset) throws InterruptedException
    {
        int requiredLength = shallowLengthRequiredForOffset(requiredOffset);
        int localMax = maxRequestedShallowLength.get();

        boolean didSucceed = false;
        //set localMax to the deep Array Length we'll have after resizing
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
        //then add our chunks
        byte[][] replacement = new byte[requiredLength][];

        for (int i = 0; i < localMax; i++)
        {
            replacement[i] = masterList[i];
        }

        for (int i = localMax; i < requiredLength; i++)
        {
            replacement[i] = new byte[ELEMENT_LENGTH<<(i-1)];
        }

        if (!masterListUpdater.compareAndSet(this, masterList, replacement))
        {
            throw new InterruptedException("Something has gone terribly wrong");
        }
    }

    AtomicResizingLongArray()
    {
        masterList = new byte[1][];
        masterList[0] = new byte[ELEMENT_LENGTH];
        maxRequestedShallowLength = new AtomicInteger(1);
    }

    public void set(int offset, byte[] contents) throws InterruptedException
    {
        ensureMasterListLongEnough(offset + contents.length);

        int writeBufferPosition = 0;
        int overallOffsetOfCurrentArray = 0;
        for (int i = 0; i < masterList.length; i++)
        {
            int lengthOfCurrentArray = ELEMENT_LENGTH << i;
            int overallEndPositionOfCurrentArray =
                overallOffsetOfCurrentArray
                +
                lengthOfCurrentArray
                ;

            //do we have anything to read in this subarray?
            if (overallEndPositionOfCurrentArray > offset)
            {
                int startOffset;

                if (overallOffsetOfCurrentArray > offset)
                {
                    startOffset = 0;
                }
                else
                {
                    startOffset = offset - overallOffsetOfCurrentArray;
                }

                int bytesToRead =
                    Math.min
                    (
                        lengthOfCurrentArray-startOffset,
                        contents.length - writeBufferPosition
                    );

                System.arraycopy
                (
                    contents,
                    writeBufferPosition,
                    masterList[i],
                    startOffset,
                    bytesToRead
                );

                writeBufferPosition += bytesToRead;
            }

            //are we done reading?
            if (overallEndPositionOfCurrentArray >= offset + contents.length)
            {
                break;
            }

            overallOffsetOfCurrentArray += lengthOfCurrentArray;
        }
    }

    public byte[] get(int offset, int length)
    {
        byte[] retVal = new byte[length];

        int readBufferPosition = 0;
        int overallOffsetOfCurrentArray = 0;
        for (int i = 0; i < masterList.length; i++)
        {
            int lengthOfCurrentArray = ELEMENT_LENGTH << i;
            int overallEndPositionOfCurrentArray =
                overallOffsetOfCurrentArray
                +
                lengthOfCurrentArray
                ;

            //do we have anything to read in this subarray?
            if (overallEndPositionOfCurrentArray > offset)
            {
                int startOffset;

                if (overallOffsetOfCurrentArray > offset)
                {
                    startOffset = 0;
                }
                else
                {
                    startOffset = offset - overallOffsetOfCurrentArray;
                }

                int bytesToRead =
                    Math.min
                    (
                        lengthOfCurrentArray-startOffset,
                        length - readBufferPosition
                    );

                System.arraycopy
                (
                    masterList[i],
                    startOffset,
                    retVal,
                    readBufferPosition,
                    bytesToRead
                );

                readBufferPosition += bytesToRead;
            }

            //are we done reading?
            if (overallEndPositionOfCurrentArray >= offset + length)
            {
                break;
            }

            overallOffsetOfCurrentArray += lengthOfCurrentArray;
        }

        return retVal;
    }

    private static final Constructor<UUID> privateUuidFromBytes;

    static
    {
        Constructor<UUID> reflected = null;
        try
        {
            reflected = UUID.class.getDeclaredConstructor(byte[].class);
            reflected.setAccessible(true);
        }
        catch (NoSuchMethodException e)
        {
            e.printStackTrace();
        }

        privateUuidFromBytes = reflected;
    }

    public static UUID uuidFromByteArr(byte[] source, int offset) throws IllegalAccessException, InvocationTargetException, InstantiationException
    {
        ByteBuffer uuidAtOffset = ByteBuffer.wrap(source, offset, ELEMENT_LENGTH);

        byte[] slice = new byte[ELEMENT_LENGTH];

        return privateUuidFromBytes.newInstance(new Object[]{slice});
    }
}
