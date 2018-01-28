package org.josh.JoshDb.MessageTree;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.josh.JoshDb.Message;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageTreeNode
{
    // todo I think the key thing in this class is that we're going to add messages
    // crazy fast all the time, but we can accept as an argument a function to
    // coalesce the messages, basically a collector that returns a coalescey
    // packy type message
    // that sums all the additions, subtracts all the subtractions,
    // but keeps all the historical data that we might want to keep for
    // security or PKI or whatever reasons
    // and we can conceivably coalesce messages atomically per subtree
    // if we do a refcounty herlihyish swap thing, though that might be
    // a bit painful
    //
    private int trieLevel;

    /**
     * In sync enough- this is just an estimate that needs to be in sync enough
     * that if we go offline and then shut down insertions it will sync up
     * so we can serialize it TODO should this be a Counter?
     */
    AtomicInteger subTreeNodeCount = new AtomicInteger(0);


    //the number of ints in a key sequence
    private final int keyLength;

    final boolean isLeaf;

    private final NonBlockingHashMap<Integer, MessageTreeNode> subtrees;

    private final NonBlockingHashMap<Integer, Message> messages;


    public MessageTreeNode(int trieLevel, int keyLength)
    {
        this.keyLength = keyLength;
        this.trieLevel = trieLevel;

        if (trieLevel == keyLength - 1)
        {
            isLeaf = true;
            messages = new NonBlockingHashMap<>();
            subtrees = null;
        }
        else
        {
            isLeaf = false;
            messages = null;
            subtrees = new NonBlockingHashMap<>();
        }
    }

    //todo boolean add and keyset
    boolean add(Message msg)
    {
        return add(new UtilityMessageWrapper(msg));
    }


    Collection<Message> getAll()
    {
       if (isLeaf)
       {
           return messages.values();
       }

       Collection<Message> subTreeMessages = new HashSet<>();
       for (MessageTreeNode subTreeNode : subtrees.values())
       {
           subTreeMessages.addAll(subTreeNode.getAll());
       }

       return subTreeMessages;
    }

    Collection<Message> get(int[] prefix)
    {
        if (prefix == null || prefix.length == 0)
        {
            throw new IllegalArgumentException("Got a prefix that doesn't make any sense");
        }

        if (prefix.length < trieLevel+1)
        {
            throw new IllegalArgumentException("Got a prefix with a length" +
                                               prefix.length +
                                               "that doesn't work for a node with depth " +
                                               trieLevel);
        }

        if (isLeaf)
        {
            return Collections.singletonList(messages.get(prefix[trieLevel]));
        }

        MessageTreeNode subTreeForPrefix = subtrees.get(prefix[trieLevel]);

        if (subTreeForPrefix == null)
        {
            //noinspection unchecked
            return Collections.EMPTY_LIST;
        }

        if (prefix.length == trieLevel+1)
        {
            return subTreeForPrefix.getAll();
        }

        return subTreeForPrefix.get(prefix);
    }


    /**
     * @param messageWrapper
     * @return whether or not the MessageTree was changed as a result of this call
     */
    private boolean add(UtilityMessageWrapper messageWrapper)
    {
        messageWrapper.setTrieLevel(trieLevel);
        subTreeNodeCount.incrementAndGet();
        if (isLeaf)
        {
            //TODO pretty sure replacing is the correct thing to do here, but time will tell
            return messages.put(messageWrapper.hashCode(), messageWrapper.data) == null;
        }

        MessageTreeNode subtreeForMessage = new MessageTreeNode(trieLevel+1, keyLength);

        MessageTreeNode existingNode = subtrees.putIfAbsent(messageWrapper.hashCode(), subtreeForMessage);

        if (existingNode == null)
        {
            existingNode = subtreeForMessage;
        }

        return existingNode.add(messageWrapper);
    }


    //todo addAll and subtraction such that a remote node can send the keyset for the
    // rootNode and we can send back our rootNode subtracted by theirs and they can
    // addAll it

    /**
     * Should really only be called on the rootNode unless you really know
     * what you're doing and have some very strange requirements
     * @return the number of messages that we already had
     */
    public int addAll(Collection<Message> msg)
    {
        return msg.stream()
                .map(UtilityMessageWrapper::new)
                .map(this::add)
                .mapToInt(insertCausedChange -> insertCausedChange ? 0 : 1)
                .sum();
    }

//    HashMap<Integer, Message> minus(/* TODO what the hell do we pass as an argument here?????? */)
//    {
//        //answer to above question: the inner class to be defined below maybe?
//
//    }




    static class KeySet
    {
        //TODO I think I remember a project where I inserted a tree structure into an
        //array in a very efficient way

    }


}
