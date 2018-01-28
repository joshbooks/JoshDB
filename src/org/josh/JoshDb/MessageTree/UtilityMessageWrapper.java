package org.josh.JoshDb.MessageTree;

import org.josh.JoshDb.Message;

import java.util.Arrays;

import static com.sangupta.murmur.Murmur3.hash_x64_128;
import static com.sangupta.murmur.Murmur3.hash_x86_32;

public class UtilityMessageWrapper
{
    public final Message data;
    private static final long murmurSeed = 0x51284249;

    public UtilityMessageWrapper(Message data)
    {
        this.data = data;
    }

    byte[] memoizedDataBytes = null;
    private byte[] bytesForData()
    {
        if (memoizedDataBytes == null)
        {
            memoizedDataBytes = new byte[]{0, 0, 0, 0};//todo
        }

        return memoizedDataBytes;
    }

    private ThreadLocal<Integer> trieLevel;

    /**
     * Not thread safe, sets the trielevel so this object
     * will return the correct hashCode for the hashmap on
     * this level of the trie
     * @param trieLevel
     */
    void setTrieLevel(int trieLevel)
    {
        this.trieLevel.set(trieLevel);
    }

    @Override
    public String toString()
    {
        return Arrays.toString(bytesForData());

    }

    private int[] memoizedFourPartHash = null;
    int[] fourPartHashCode()
    {
        if (memoizedFourPartHash == null)
        {
            long[] longs = hash_x64_128(bytesForData(), bytesForData().length, murmurSeed);
            memoizedFourPartHash =
                    new int[]
                    {
                        (int) (longs[0] >> 32),
                        (int) (longs[0]),
                        (int) (longs[1] >> 32),
                        (int) (longs[1])
                    };
        }

        return memoizedFourPartHash;
    }

    long longHashCode()
    {
        return hash_x86_32(bytesForData(), bytesForData().length, murmurSeed);
    }

    @Override
    public int hashCode()
    {
        return fourPartHashCode()[trieLevel.get()];
    }
}
