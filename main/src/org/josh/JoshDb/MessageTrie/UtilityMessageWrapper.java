package org.josh.JoshDb.MessageTrie;

import org.josh.JoshDb.Message;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class UtilityMessageWrapper
{
    public final Message data;
    private final int[] fourPartHashCode;
    private final ThreadLocal<Integer> trieLevel = new ThreadLocal<>();

    public UtilityMessageWrapper(Message data)
    {
        this.data = data;
        fourPartHashCode =
            new int[]
            {
                (int) (data.messageId.getMostSignificantBits() >> 32),
                (int) data.messageId.getMostSignificantBits(),
                (int) (data.messageId.getLeastSignificantBits() >> 32),
                (int) data.messageId.getLeastSignificantBits()
            };
    }

    private static byte[] bytesForData(UtilityMessageWrapper wrapper)
    {
        int length = messageLength(wrapper);
        ByteBuffer buffer = ByteBuffer.allocateDirect(length);

        //todo insert primitiveTypes, structs, and lists of same using
        // standard ByteBuffer operations and loops

        //todo this might work better as an EnumMap of function pointers
        switch (wrapper.data.type)
        {
            //todo some recursive cases where we go
            // bytesForData(new UtilityMessageWrapper((Message) wrapper.data.data);
            //and some more boring cases
        }

        return buffer.array();
    }

    private static int messageLength(UtilityMessageWrapper wrapper)
    {
        int length = 0;

        //todo size of all primitive types/structs and lists of same

        //todo this might work better as an enum map of function pointers
        switch (wrapper.data.type)
        {
            //todo some nice recursion in here to determine the size of data.data
        }

        return length;
    }

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

    @Override
    public int hashCode()
    {
        Integer localTrieLevel = trieLevel.get();

        if (localTrieLevel == null)
        {
            localTrieLevel = 0;
        }

        int hash = fourPartHashCode[localTrieLevel];

        trieLevel.set(localTrieLevel + 1);

        return hash;
    }
}
