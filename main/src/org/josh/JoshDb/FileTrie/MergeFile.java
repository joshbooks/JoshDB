package org.josh.JoshDb.FileTrie;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.josh.JoshDb.ArcCloseable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MergeFile
{
    // hacky nonsense, should be dynamically determined
    // based on the underlying filesystem, but using ext3 for now
    // because that's what my laptop runs
    public static final int PIPE_BUF = 4096;

    // go ahead and take up 8 bytes, you earned it
    public static final byte[] OBJECT_BEGIN = new byte[]{'f', 'u', 'c', 'k'};

    public static final byte[] PAGE_BEGIN = new byte[]{'d', 'a', 'm', 'n'};

    public static final byte[] OBJECT_END = new byte[]{'s', 'h', 'i', 't'};

    public static final byte[] PAGE_END = new byte[]{'z', '/', 'o', 's'};

    // all objects representations stored to file will take up a positive
    // integer multiple of PIPE_BUF.
    // each page must start with OBJECT_BEGIN or PAGE_BEGIN and end with
    // PAGE_END or OBJECT_END
    // If OBJECT_END occurs before the end of the page, the remainder of the page
    // must be filled with zeroes up to PAGE_END

    public static final NonBlockingHashMap<Path, ArcCloseable<OutputStream>> writerForFile =
            new NonBlockingHashMap<>();

    static ArcCloseable<OutputStream> getWriter(Path file) throws IOException
    {
        assert file != null;
        ArcCloseable<OutputStream> existing = writerForFile.get(file);

        //increment for the thread calling this function
        while (existing == null || !existing.incRef())
        {
            OutputStream newWriter = Files.newOutputStream(file, StandardOpenOption.APPEND);
            ArcCloseable<OutputStream> replacement = new ArcCloseable<>(newWriter);
            //increment for map reference
            replacement.incRef();

            if (existing == null)
            {
                writerForFile.putIfAbsent(file, replacement);
                existing = writerForFile.get(file);
            }
            else if (!writerForFile.replace(file, existing, replacement))
            {//basically this case should only come up if a thread dies while expiring a writer
                replacement.decRef();
                existing = writerForFile.get(file);
            }
        }

        return existing;
    }

    public static void doneWithWriter(ArcCloseable<OutputStream> writer)
    {
        writer.decRef();
    }

    public static void writePage(Path file, byte[] page) throws IOException
    {
        assert page.length == PIPE_BUF;

        for (int i = 0; i < 4; i++)
        {
            assert page[i] == PAGE_BEGIN[i] || page[i] == OBJECT_BEGIN[i];

            int endLessI = (PIPE_BUF) - (4 - i);

            assert page[endLessI] == PAGE_END[i] || page[endLessI] == OBJECT_END[i];
        }

        ArcCloseable<OutputStream> writer = getWriter(file);
        writer.getBacking().write(page);
        doneWithWriter(writer);
    }


    public static void writeSerializedObject(Path file, byte[] serializedObject) throws IOException
    {
        // this allocation makes me a little sad
        byte[] page = new byte[4096];

        int length = serializedObject.length;

        if (length <= (PIPE_BUF - 12)) //most common single page case
        {
            System.arraycopy(OBJECT_BEGIN, 0, page, 0, 4);
            System.arraycopy(serializedObject, 0, page, 4, length);
            System.arraycopy(OBJECT_END, 0, page, length + 4, 4);
            System.arraycopy(PAGE_END, 0, page, PIPE_BUF - 4, 4);
            writePage(file, page);
        }
        if (length == (PIPE_BUF - 8)) //very lucky single page case
        { //"magic number elision" is a great name for a band
            System.arraycopy(OBJECT_BEGIN, 0, page, 0, 4);
            System.arraycopy(serializedObject, 0, page, 4, length);
            System.arraycopy(OBJECT_END, 0, page, length + 4, 4);
            writePage(file, page);
        }
        else // multi page case
        {
            // yes you did read the logic correctly, the length of the object being
            //between 4096-8 and 4096-12 does in fact mean we're going to waste
            //almost a whole page so we can finish writing the OBJECT_END magic bytes
            //I don't like it any more than you do but it's the only way I can
            //think of right now to guarantee the properties that I'm after
            
        }






    }


}
