package org.josh.JoshDB.FileTrie;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.APPEND;

public class SanityCheck
{
    // wtf why does this still work? this should not work like this
    // is java doing some fucking locking bullshit behind my back????
    // why do you do this java? why do you betray me like this?
    // if so further support for rewriting in a lower level language
    // does c have a disruptor type thing?
    // maybe rust... maybe... maybe, the rust dude told me tokio was
    // going to start sucking less soon, if that's true it might be
    // worth looking into, but I miss C, I miss it with a passion
    public static final int PIPE_BUF = 4096*4;

//    public static OutputStream sharedWriter;

    public static AtomicInteger writerCount = new AtomicInteger(0);

    public static final Path testFile = Paths.get("./testAtomicWrites");


    public static Runnable threadWriterFunction =
        () ->
        {
            OutputStream privateWriter = null;
            try
            {
                privateWriter = Files.newOutputStream(testFile, APPEND);
            }
            catch (IOException e)
            {
                System.out.println("Couldn't open up a writer");
                e.printStackTrace();
                System.exit(-1);
            }
            byte myNumber = (byte) writerCount.getAndIncrement();

            System.out.println("My thread got " + myNumber + " going to write a bunch of those now");

            byte[] buffer = new byte[PIPE_BUF];
            Arrays.fill(buffer, myNumber);

            for (int i = 0; i < 10; i++)
            {
//                try
//                {
//                    Thread.sleep(new Random().nextInt(256));
//                }
//                catch (InterruptedException e)
//                {
//                    e.printStackTrace();
//                }

                // in decent code this try would be outside the loop,
                // but it's a sanity check thing so like whatever
                try
                {
                    privateWriter.write(buffer);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            try
            {
                privateWriter.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        };


    public static void main(String[] args) throws IOException, InterruptedException
    {

        if (!Files.exists(testFile))
            Files.createFile(testFile);

//        sharedWriter = Files.newOutputStream(testFile, APPEND);

        int numThreads = 10;
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++)
        {
            threads[i] = new Thread(threadWriterFunction);
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++)
        {
            threads[i].join();
        }

        //sharedWriter.close();


        //now we want to make sure things aren't interleaved like they shouldn't be

        InputStream in = Files.newInputStream(testFile);

        byte[] buffer = new byte[PIPE_BUF];

        int iterCount = 0;
        while (true)
        {
            if (in.available() <= 0)
            {
                System.out.println("All done with file");
                break;
            }

            System.out.println
            (
                "We have at least some bytes to read, beginning iteration "
                +
                iterCount
            );

            int bytesRead = in.read(buffer);
            if (bytesRead != PIPE_BUF)
            {
                System.out.println("Something is very wrong, we read less than PIPE_BUF");
                System.exit(-2);
            }

            byte firstByte = buffer[0];

            for (int i = 1; i < PIPE_BUF; i++)
            {
                if (buffer[i] != firstByte)
                {
                    System.out.println("caught some naughty interleaving, that's no good");
                }
            }

            iterCount++;
        }

        in.close();

        Files.delete(testFile);
    }
}
