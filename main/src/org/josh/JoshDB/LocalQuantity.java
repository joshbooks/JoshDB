package org.josh.JoshDB;

import java.util.concurrent.atomic.AtomicLong;

public class LocalQuantity
{
    //todo use Cliff Click's Counter class here
    //for better multithreaded performance
    AtomicLong inMemoryQuantity;

    public LocalQuantity(){this(0);}

    // todo someday this will get loaded from persistent storage
    public LocalQuantity(long initialValue)
    {
        inMemoryQuantity = new AtomicLong(initialValue);
    }

    /**
     * Give away some {@code amount} of stuff from our own local stash of stuff
     * @param amount the amount of stuff to give away
     * @return whether or not it was given
     */
    public boolean giveAway(long amount) {
        ensureSensibleAmount(amount);

        long localValue, amountAfterRequest;

        do
        {
            localValue = inMemoryQuantity.get();

            if (localValue < amount)
            {
                return false;
            }
            amountAfterRequest = localValue - amount;
        } while (!inMemoryQuantity.compareAndSet(localValue, amountAfterRequest));

        return true;
    }



    private static void ensureSensibleAmount(long amount)
    {
        //"common sense" limits
        if (amount < 0 || amount >= Long.MAX_VALUE / 2)
        {
            throw new IllegalArgumentException("You know what you did, " +
                                               "and you should feel shame");
        }
    }


    public void receive(long amount)
    {
        ensureSensibleAmount(amount);

        inMemoryQuantity.getAndAdd(amount);
    }


    public long currentEstimate()
    {
        return inMemoryQuantity.get();
    }
}
