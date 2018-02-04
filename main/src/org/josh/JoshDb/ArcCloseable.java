package org.josh.JoshDb;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ArcCloseable<T extends Closeable>
{
    private final AtomicInteger refs = new AtomicInteger(0);

    private final T backingCloseable;

    private final AtomicBoolean closingUp = new AtomicBoolean(false);

    public T getBacking()
    {
        return backingCloseable;
    }

    public ArcCloseable(T closeable)
    {
        backingCloseable = closeable;
    }

    /**
     *
     * @return whether or not we were able to successfully increment
     * the number of references to this org.josh.JoshDb.ArcCloseable. So true if the number
     * of references was incremented, false if this backingCloseable is about to get
     * closed
     */
    public boolean incRef()
    {
        int currentValue;

        do
        {
            currentValue = refs.get();

            if (currentValue == 0 && closingUp.get())
            {
                //closingUp should never go from true to false
                logAssist(currentValue, closingUp.get());
                return false;
            }
        } while(!refs.compareAndSet(currentValue, currentValue+1));

        return true;
    }

    class ArcState
    {
        //java's sequential memory model is cool
        int currentRefCount;
        boolean closingUp;
    }

    /**
     * Logger method to contain the big nasty switch statement,
     * maps
     * @param currentValue
     * @param b
     */
    private void logAssist(int currentValue, boolean b)
    {

    }


    public void decRef()
    {
        int currentValue = refs.decrementAndGet();
        boolean isClosing = closingUp.get();

        while (currentValue <= 0 && !isClosing)
        {
            if (closingUp.compareAndSet(false, true))
            {
                try
                {
                    backingCloseable.close();
                }
                catch (IOException e)
                {
                    //todo log something
                }
            }

            isClosing = closingUp.get();
            currentValue = refs.get();
        }
    }

    //todo really ought to do a registry thing here so the same path
    // always gets the same ArcCloseable
    public static ArcCloseable<BufferedWriter> arcWriterForFile(Path file)
            throws IOException
    {
        if (!Files.exists(file.getParent()))
        {
            Files.createDirectories(file.getParent());
        }

        if (!Files.exists(file))
        {
            Files.createFile(file);
        }

        return new ArcCloseable<>(Files.newBufferedWriter(file));
    }
}
