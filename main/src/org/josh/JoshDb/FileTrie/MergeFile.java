package org.josh.JoshDb.FileTrie;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.josh.JoshDb.ArcCloseable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MergeFile implements Iterable<byte[]>
{
    // all objects representations stored to file will take up a positive
    // integer multiple of PIPE_BUF.
    // each page must start with PAGE_BEGIN and end with
    // PAGE_END, all object representations shall begin with a long containing the
    // object sequence number of that object (obtained by calling
    // objectSequenceNumber.getAndIncrement()) followed by OBJECT_BEGIN and end with
    // OBJECT_END
    // If OBJECT_END occurs before the end of the page, the remainder of the page
    // must be filled with zeroes up to PAGE_END if object end occurs less than 4 bytes
    // before PAGE_END as many bytes of OBJECT_END as can be written in that page shall
    // be written and the rest will be written on the next page, followed by zeroes up
    // until PAGE_END of the following page

    // hacky nonsense, should be dynamically determined
    // based on the underlying filesystem, but using ext3 for now
    // because that's what my laptop runs
    public static final int PIPE_BUF = 4096;

    public static final byte[] OBJECT_BEGIN = new byte[]{'f', 'u', 'c', 'k'};

    public static final byte[] PAGE_BEGIN = new byte[]{'d', 'a', 'm', 'n'};

    public static final byte[] OBJECT_END = new byte[]{'s', 'h', 'i', 't'};

    public static final byte[] PAGE_END = new byte[]{'z', '/', 'o', 's'};

    public static final byte[] OBJECT_CONTINUE = new byte[]{'m', 'u', 'n', 'g'};

    private final Path file;

    // todo it seems like instantiation is pretty much just done to implement iterator
    // maybe that should be split into a different class??
    private static NonBlockingHashMap<Path, MergeFile> registry =
            new NonBlockingHashMap<>();

    public static MergeFile mergeFileForPath(Path path)
    {
        MergeFile temp = new MergeFile(path);

        MergeFile shared = registry.putIfAbsent(path, temp);

        if (shared == null)
        {
            shared = temp;
        }

        return shared;
    }

    private MergeFile(Path file)
    {
        this.file = file;
    }

    @Override
    public Iterator<byte[]> iterator()
    {
        //not thread safe, the notion is that input is always positioned
        // to point to the next available object or the end of the file
        RandomAccessFile input;

        try
        {
            input = new RandomAccessFile(file.toFile(), "r");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new UncheckedIOException
            (
                "Unable to open an InputStream for this MergeFile",
                e
            );
        }

        final RandomAccessFile effectiveIn = input;

        return new Iterator<byte[]>()
        {
            @Override
            public boolean hasNext()
            {
                try
                {
                    return effectiveIn.getFilePointer() < effectiveIn.length();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    throw new UncheckedIOException
                    (
                        "Failed to ascertain available bytes for this MergeFile",
                        e
                    );
                }
            }

            @Override
            public byte[] next()
            {
                try
                {
                    return arrayFromList(pageListForNextObject(effectiveIn));
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    return null;
                }
            }
        };
    }

    private byte[] arrayFromList(List<byte[]> list)
    {
        int totalSize = list.stream().mapToInt(array -> array.length).sum();

        byte[] array = new byte[totalSize];

        int position = 0;
        for (byte[] page : list)
        {
            System.arraycopy(page, 0, array, position, page.length);

            position += page.length;
        }

        return array;
    }

    //todo need to position effectiveIn at the beginning of the next object,
    //which-- due to our fancy atomic append scheme-- might be the very next page
    private List<byte[]> pageListForNextObject(RandomAccessFile effectiveIn) throws IOException
    {
        List<byte[]> delimited = allPagesForNextEntry(effectiveIn);

        List<byte[]> cleaned = new ArrayList<>(delimited.size());

        long sequenceNumber = -1;
        boolean wraparound = false;

        for (int i = 0; i < delimited.size(); i++)
        {
            byte[] delimitedPage = delimited.get(i);

            //all pages should begin with PAGE_BEGIN then the sequence number
            assert ByteBuffer.wrap(delimitedPage, 0, 4).getInt() == PAGE_BEGIN_INT;
            long thisPageSequenceNumber = ByteBuffer.wrap(delimitedPage, 4, 8).getLong();

            int objectDataStart;

            if (i == 0)
            {
                sequenceNumber = thisPageSequenceNumber;
                assert ByteBuffer.wrap(delimitedPage, 12, 4).getInt() == OBJECT_BEGIN_INT;
                objectDataStart = 16;
            }
            else
            {
                assert thisPageSequenceNumber == sequenceNumber;
                objectDataStart = 12;
            }


            //if this isn't the last page or the second to last page we
            // want all the data up to PAGE_END
            int objectDataEnd = delimitedPage.length - 4;

            //if this is the last or second to last page we want to know where OBJECT_END is
            if (i >= delimited.size() - 2)
            {
                if (i == delimited.size() - 1 && wraparound)
                {
                    //todo handle validation of the last page
                    break;
                }

                // handle the regular last page case where we have
                // OBJECT_END zeroes, then PAGE_END
                if (i == delimited.size() - 1)
                {
                    //go from the beginning of object data up to the start of PAGE_END
                    for (int k = objectDataStart; k < delimitedPage.length - 4; k++)
                    {
                        if (delimitedPage[k] == OBJECT_END[0])
                        {
                            if (delimitedPage[k+1] == OBJECT_END[1])
                            {
                                if (delimitedPage[k+2] == OBJECT_END[2])
                                {
                                    if (delimitedPage[k+3] == OBJECT_END[3])
                                    {
                                        objectDataEnd = k;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                //handle the wraparound case for the second to last page
                if (i == delimited.size() - 2)
                {
                    int byteNumberEncountered = -1;
                    for (int k = 0; k < 4; k++)
                    {
                        if (delimitedPage[delimitedPage.length - 5] == OBJECT_END[k])
                        {
                            byteNumberEncountered = k + 1;
                        }
                    }

                    //wraparound case
                    if (byteNumberEncountered != -1)
                    {
                        objectDataEnd = (delimitedPage.length - 4) - byteNumberEncountered;

                        wraparound = true;
                    }
                }
            }

            byte[] cleanPage = new byte[objectDataEnd - objectDataStart];

            System.arraycopy(delimitedPage, objectDataStart, cleanPage, 0, cleanPage.length);

            cleaned.add(i, cleanPage);
        }

        return cleaned;
    }

    public static final NonBlockingHashMap<Path, ArcCloseable<OutputStream>> writerForFile =
            new NonBlockingHashMap<>();

    public static final NonBlockingHashMap<Path, AtomicLong> objectCountForFile =
            new NonBlockingHashMap<>();

    /**
     * Every time you want to write an object to a MergeFile, you just
     * getObjectCount and then
     * getAndAdd that guy and write out the resulting long after each
     * PAGE_BEGIN marker for that object, that way we can keep track
     * of which Object each page corresponds to
     */
    private static AtomicLong getObjectCount(Path file)
    {
        // todo decide whether this or getMergeFileForPath's logic is better,
        // factor out, and reuse in the other
        assert file != null;

        AtomicLong existing = objectCountForFile.get(file);

        while (existing == null)
        {
            objectCountForFile.putIfAbsent(file, new AtomicLong(0));
            existing = objectCountForFile.get(file);
        }

        return existing;
    }

    private static ArcCloseable<OutputStream> getWriter(Path file) throws IOException
    {
        assert file != null;
        ArcCloseable<OutputStream> existing = writerForFile.get(file);

        //increment for the thread calling this function
        while (existing == null || !existing.incRef())
        {
            OutputStream newWriter = Files.newOutputStream
            (
                file,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE,
                StandardOpenOption.SYNC
            );

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

    public static void validateAndWriteDelimitedPage(Path file, byte[] page) throws IOException
    {
        assert page.length == PIPE_BUF;

        for (int i = 0; i < 4; i++)
        {
            // todo use a proper array comparison function here and
            // do two rounds so even if the compiler is super literal
            // we won't do dirty things to the cache without cause
            // reading the beginning and end skipping the middle bit
            // is bad enough

            assert page[i] == PAGE_BEGIN[i];

            int endLessI = (PIPE_BUF) - (4 - i);

            assert page[endLessI] == PAGE_END[i];
        }

        ArcCloseable<OutputStream> writer = null;
        try
        {
            writer = getWriter(file);
            writer.getBacking().write(page);
        }finally
        {
            if (writer != null)
            {
                doneWithWriter(writer);
            }
        }
    }

    private static final int PAGE_END_POSITION = PIPE_BUF - PAGE_END.length;

    public static void writeSerializedObject(Path file, byte[] serializedObject) throws IOException
    {
        long sequenceNumber = getObjectCount(file).getAndIncrement();

        int objectPosition = 0;
        int objectLength = serializedObject.length;


        int objectEndBytesWritten = 0;

        while (objectEndBytesWritten != OBJECT_END.length)
        {
            byte[] page = new byte[PIPE_BUF];
            int pagePosition = 0;

            //PAGE_BEGIN
            System.arraycopy(PAGE_BEGIN, 0, page, pagePosition, PAGE_BEGIN.length);
            pagePosition += PAGE_BEGIN.length;

            //sequence number
            ByteBuffer.wrap(page, PAGE_BEGIN.length, 8).putLong(sequenceNumber);
            pagePosition += 8;

            // OBJECT_BEGIN for first page
            if (objectPosition == 0)
            {
                System.arraycopy(OBJECT_BEGIN, 0, page, pagePosition, OBJECT_BEGIN.length);
                pagePosition += OBJECT_BEGIN.length;
            }

            int remainingObjectLength = objectLength - objectPosition;
            int remainingPageLength = (PIPE_BUF - PAGE_END.length) - pagePosition;

            //int leftover = remainingPageLength - remainingObjectLength;


            //write the rest of the object, then OBJECT_END
            if (remainingObjectLength + OBJECT_END.length <= remainingPageLength)
            {
                System.arraycopy
                (
                    serializedObject,
                    objectPosition,
                    page,
                    pagePosition,
                    remainingObjectLength
                );
                pagePosition += remainingObjectLength;
                objectPosition += remainingObjectLength;

                int objectEndBytesToWrite = OBJECT_END.length - objectEndBytesWritten;

                System.arraycopy
                (
                    OBJECT_END,
                    objectEndBytesWritten,
                    page,
                    pagePosition,
                    objectEndBytesToWrite
                );
                pagePosition += objectEndBytesToWrite; //debugging
                //could just assign, but this is nicer for debugging so leaving it for now
                objectEndBytesWritten += objectEndBytesToWrite;
            }
            //write the whole page up to PAGE_END
            else if (remainingObjectLength >= remainingPageLength)
            {
                System.arraycopy
                (
                    serializedObject,
                    objectPosition,
                    page,
                    pagePosition,
                    remainingPageLength
                );

                objectPosition += remainingPageLength;
                pagePosition += remainingPageLength;

                assert pagePosition == PAGE_END_POSITION;
            }
            //wraparound case, write the rest of the object and then at least one byte of OBJECT_END
            else if (remainingObjectLength < remainingPageLength)
            {
                //this one I might have to think about for a minute
                int amountToWrite = remainingPageLength - PAGE_END.length;
                System.arraycopy(serializedObject, objectPosition, page, pagePosition, amountToWrite);
                //objectPosition should be serializedObject.length after this
                objectPosition += amountToWrite;
                pagePosition += amountToWrite;

                int objectEndBytesToWrite = (PIPE_BUF - PAGE_END.length) - pagePosition;
                System.arraycopy(OBJECT_END, 0, page, pagePosition, objectEndBytesToWrite);
                objectEndBytesWritten += objectEndBytesToWrite;
                pagePosition += objectEndBytesToWrite;

                assert pagePosition == PAGE_END_POSITION;
            }
            else
            {
                System.out.println("Something has gone terribly wrong");
                assert false;
            }

            System.arraycopy(PAGE_END, 0, page, PAGE_END_POSITION, PAGE_END.length);

            validateAndWriteDelimitedPage(file, page);
        }
    }

//    public static void writeSerializedObject(Path file, byte[] serializedObject) throws IOException
//    {
//        int remainingLength = serializedObject.length;
//        int position = 0;
//        boolean isFirstIteration = true;
//
//        //todo so we need to write another 8 bytes of object count number
//        //todo and write out object continue when it isn't object begin
//        //but first let's do a little sanity check and make sure my scheme
//        // for atomc writes will work at all
//        while (remainingLength > -4)
//        {
//            // this allocation makes me a little sad
//            //if we explicitly write zeroes in the cases
//            // that require runs of zeroes we get rid of it
//            byte[] page = new byte[PIPE_BUF];
//
//            if (remainingLength <= 0)
//            {
//                //then we just have to write the remaining bit of OBJECT_END
//
//                // yes you did read the logic correctly, the length of the object being
//                //between 4096-8 and 4096-12 does in fact mean we're going to waste
//                //almost a whole page so we can finish writing the OBJECT_END magic bytes
//                //I don't like it any more than you do but it's the only way I can
//                //think of right now to guarantee the properties that I'm after
//
//                System.arraycopy(OBJECT_END, -remainingLength, page, 0, remainingLength + 4);
//                writePage(file, page);
//                remainingLength = -4;
//            }
//            else if (remainingLength <= (PIPE_BUF - 12))
//            {
//                System.arraycopy(OBJECT_BEGIN, 0, page, 0, 4);
//                System.arraycopy(serializedObject, 0, page, 4, remainingLength);
//                System.arraycopy(OBJECT_END, 0, page, remainingLength + 4, 4);
//                System.arraycopy(PAGE_END, 0, page, PIPE_BUF - 4, 4);
//                writePage(file, page);
//                remainingLength = -4;
//            }
//            else if (remainingLength == (PIPE_BUF - 8))
//            { //"magic number elision" is a great name for a band
//                System.arraycopy(OBJECT_BEGIN, 0, page, 0, 4);
//                System.arraycopy(serializedObject, 0, page, 4, remainingLength);
//                System.arraycopy(OBJECT_END, 0, page, remainingLength + 4, 4);
//                writePage(file, page);
//                remainingLength = -4;
//            } else // multi page case
//            {
//                if (isFirstIteration)
//                {
//                    System.arraycopy(OBJECT_BEGIN, 0, page, 0, 4);
//                }
//                else
//                {
//                    System.arraycopy(PAGE_BEGIN, 0, page, 0, 4);
//                }
//
//                System.arraycopy(serializedObject, position, page, 4, (PIPE_BUF - 4));
//                position += (PIPE_BUF - 4);
//                remainingLength = (PIPE_BUF - 4);
//
//                System.arraycopy(PAGE_END, 0, page, PIPE_BUF - 4, 4);
//
//                writePage(file, page);
//            }
//            isFirstIteration = false;
//        }
//    }

//    public static void writeSerializedObject(Path file, byte[] serializedObject) throws IOException
//    {
//        byte[] page = new byte[PIPE_BUF];
//        boolean firstPage = true;
//        long objectNumber = getObjectCount(file).getAndIncrement();
//
//        System.arraycopy(PAGE_BEGIN, 0, page, 0, 4);
//        System.arraycopy(PAGE_END, 0, page, PIPE_BUF - 4, 4);
//
//        int objectBytesWritten = 0;
//
//        while (true)//todo actual control structure
//        {
//            byte[] pageType = firstPage ? OBJECT_BEGIN : OBJECT_CONTINUE;
//            System.arraycopy(pageType, 0, page, 4, 4);
//
//            //todo make sure this will actually put the long into the byte[] the way we want to
//            ByteBuffer.wrap(page, 8 , 8).putLong(objectNumber);
//
//            // ok so here we have a buffer of size PIPE_BUF with the first 16
//            // bytes filled as well as the last 4, so we have PIPE_BUF - 20 bytes
//            // available
//            int objectBytesRemaining = serializedObject.length - objectBytesWritten;
//
//
//            //todo I'm pretty sure I'm missing a case here, the
//            // one where OBJECT_END wraps around, but today was
//            // a work day so my brain is a little too fired to
//            // figure out how to check for it right now
//            // however I still stand by rewriting the version of
//            // this function I wrote in an airport bar
//
//            //if it's going to be OBJECT_END (0)* PAGE_END
//            if (objectBytesRemaining <= PIPE_BUF - 24)
//            {
//                System.arraycopy
//                (
//                    serializedObject,
//                    objectBytesWritten,
//                    page,
//                    16,
//                    objectBytesRemaining
//                );
//
//                System.arraycopy
//                (
//                    OBJECT_END,
//                    0,
//                    page,
//                    objectBytesWritten + objectBytesRemaining,
//                    4
//                );
//
//                //all done
//                objectBytesWritten = serializedObject.length;
//
//                // todo zero out bytes in between OBJECT_END and PAGE_END
//                // if necessary
//            }
//            // let's get rid of the whole magic number elision thing for now
//            // we might add it back in later, but for now I'd like to to keep
//            // the logic clearer
//            else
//            {
//                System.arraycopy(serializedObject, objectBytesWritten, page, 16, PIPE_BUF - 24);
//                System.arraycopy(OBJECT_END, 0, page, PIPE_BUF-8, 4);
//
//                objectBytesWritten += PIPE_BUF - 24;
//            }
//
//            firstPage = false;
//        }
//
//        writePage(file, page);
//    }

    private static final int PAGE_BEGIN_INT = ByteBuffer.wrap(PAGE_BEGIN).getInt();
    private static final int OBJECT_BEGIN_INT = ByteBuffer.wrap(OBJECT_BEGIN).getInt();
    private static final int PAGE_END_INT = ByteBuffer.wrap(PAGE_END).getInt();
    private static final int OBJECT_END_INT = ByteBuffer.wrap(OBJECT_END).getInt();

    // todo so the idea is that after this function we just need to eliminate the
    // magic numbers and we have the byte[] for the serialized object
    // (I know it's terribly wasteful, I'll make it better later I promise)
    public static List<byte[]> allPagesForNextEntry(RandomAccessFile in) throws IOException
    {
        ArrayList<byte[]> objectPages = new ArrayList<>();
        byte[] nextPage = new byte[PIPE_BUF];
        boolean lastPage = false;

        int bytesRead = in.read(nextPage);
        assert bytesRead == PIPE_BUF;

        assert ByteBuffer.wrap(nextPage, 0, 4).getInt() == PAGE_BEGIN_INT;
        // in should always be pointing to the page at the beginning of an
        // object when passed to this function
        assert ByteBuffer.wrap(nextPage, 4, 4).getInt() == OBJECT_BEGIN_INT;
        assert ByteBuffer.wrap(nextPage, (PIPE_BUF - 4), 4).getInt() == PAGE_END_INT;

        long objectCountNumber = ByteBuffer.wrap(nextPage, 8, 8).getLong();

        objectPages.add(nextPage);

        lastPage =
            ByteBuffer
                .wrap(nextPage, (PIPE_BUF - 8), 4)
                .getInt()
            ==
            OBJECT_END_INT;

        if (lastPage)
        {
            return objectPages;
        }

        nextPage = new byte[PIPE_BUF];
        long pointerToNextObject = in.getFilePointer();
        boolean confidentlySetPointerToNextObject = false;

        while (true)
        {
            long lastPagePointer = in.getFilePointer();

            bytesRead = in.read(nextPage);

            if (bytesRead < PIPE_BUF)
            {
                throw new IOException("Exhausted InputStream");
            }

            assert PAGE_BEGIN_INT == ByteBuffer.wrap(nextPage, 0, 4).getInt();

            //todo assert page ends with PAGE_END

            // todo might have to waste another 4 bytes after PAGE_BEGIN
            // to avoid collisions between the top 4 bytes of
            // objectCountNumber and OBJECT_BEGIN
            boolean sameObjectCountNumber =
                    objectCountNumber
                    ==
                    ByteBuffer.wrap(nextPage, 4, 8).getLong();

            if (!confidentlySetPointerToNextObject && !sameObjectCountNumber)
            {
                pointerToNextObject = lastPagePointer;
                confidentlySetPointerToNextObject = true;
            }

            if (!sameObjectCountNumber)
            {
                continue;
            }

            objectPages.add(nextPage);

            lastPage =
                ByteBuffer
                    .wrap(nextPage, (PIPE_BUF - 8), 4)
                    .getInt()
                ==
                OBJECT_END_INT;

            if (lastPage)
            {
                if (confidentlySetPointerToNextObject)
                {
                    in.seek(pointerToNextObject);
                }

                break;
            }

            nextPage = new byte[PIPE_BUF];
        }

        return objectPages;
    }




}
