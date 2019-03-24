package org.josh.JoshDB.FileTrie;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.josh.JoshDB.ArcCloseable;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MergeFile implements Iterable<byte[]>
{
    static
    {
        try
        {
            System.load("/home/flatline/src/JoshDB/jniUtilLib.solib");
        }
        catch (UnsatisfiedLinkError e)
        {
            // TODO I guess NativeUtils is going to be the best way to
            // load a jni lib from a jar, then have System.load as a fallback
            // for testing. A static block with that logic seems like something
            // that could be automagically inserted with a bach script :D
            System.out.println("Couldn't load native lib");
            System.exit(1);
        }
    }

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

    private final Path file;
    private final AtomicResizingLongArray sequenceArray;
    private ThreadLocal<Long> fd = new ThreadLocal<>();

    private static native long openFile(String filePath);

    // todo this should be a Closeable so we can use the nice ArcWrapper we wrote
    // and the close method should call this natjve method on fd if fd isn't -1
    private static native long closeFile(long fd);

    private static native long appendToFile(long fd, byte[] bytes, long numBytes);

    // could do this in native code but like nah
    long appendToFileHelper(byte[] bytes) throws IOException
    {
        assert bytes.length <= PIPE_BUF;
        System.out.println("starting with fd " + fd.get());
        if (fd.get() == null || fd.get() <= 0)
        {
            fd.set(openFile(file.toAbsolutePath().normalize().toString()));
            if (fd.get() <= 0)
            {
                throw new IOException("failed to open " + file + " with error " + fd);
            }
        }

        long retVal =
            appendToFile
            (
                fd.get(),
                bytes,
                bytes.length
            );

        assert retVal == bytes.length;

        return retVal;
    }

    // todo it seems like instantiation is pretty much just done to implement iterator
    // maybe that should be split into a different class??
    private static NonBlockingHashMap<Path, MergeFile> registry =
            new NonBlockingHashMap<>();

    private static org.slf4j.Logger log = LoggerFactory.getLogger(MergeFile.class.getName());

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

    private static final NonBlockingHashMap<Path, AtomicResizingLongArray> pathToSequenceArray =
            new NonBlockingHashMap<>();

    public static AtomicResizingLongArray sequenceArrayForPath(Path path)
    {
        AtomicResizingLongArray temp = new AtomicResizingLongArray();

        AtomicResizingLongArray shared = pathToSequenceArray.putIfAbsent(path, temp);

        if (shared == null)
        {
            shared = temp;
        }

        return shared;
    }


    private MergeFile(Path file)
    {
        this.file = file;
        this.sequenceArray = sequenceArrayForPath(file);
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

    static long alignRandomAccessFileToPipeBuf(RandomAccessFile in, boolean shouldLog) throws IOException
    {
        long position = in.getFilePointer();
        long mod = position % PIPE_BUF;
        if (mod != 0)
        {
            if (shouldLog)
            {
                log.warn("input file pointer was misaligned in pagePositionsForNextObject");
            }
            long aligner = position + (PIPE_BUF - mod);
            in.seek(aligner);
            position += aligner;
        }

        return position;
    }

    static int numberOfPagesForSerializedObject(int length)
    {
        int usabePageLength = PIPE_BUF - PAGE_END.length - PAGE_BEGIN.length - Long.BYTES;
        // That last long is for the sequence number
        length += OBJECT_BEGIN.length + OBJECT_END.length;

        int numPages = length / usabePageLength;
        numPages += length % usabePageLength != 0 ? 1 : 0;

        System.out.println("Expected number of pages for " + length + " is " + numPages);

        return numPages;
    }

    private static List<Long> pagePositionsForNextObject(RandomAccessFile in, int numPages) throws IOException
    {
        long startingSequenceNumber = -1;
        List<Long> pagePositions = new ArrayList<>();

        for
        (
            boolean firstIter = true;
            pagePositions.size() < numPages;
            alignRandomAccessFileToPipeBuf(in, false)
        )
        {
            alignRandomAccessFileToPipeBuf(in, firstIter);

            long pagePosition = in.getFilePointer();

            //seek to objectSequenceNumber
            in.seek(pagePosition + PAGE_BEGIN.length);
            long objectSequenceNumber = in.readLong();

            if (firstIter)
            {
                startingSequenceNumber = objectSequenceNumber;
                firstIter = false;
            }

            if (objectSequenceNumber == startingSequenceNumber)
            {
                pagePositions.add(pagePosition);
            }
        }

        return pagePositions;
    }

    private static int numberOfEndBytes(byte[] bytes)
    {
        assert bytes.length == OBJECT_END.length;

        int numBytes = 0;
        byte lastByte = bytes[bytes.length - 1];

        for (int i = 0; i < OBJECT_END.length; i++)
        {
            if (lastByte == OBJECT_END[i])
            {
                if (i == 0)
                {
                    //There was one byte and we validated it
                    return 1;
                }
                numBytes = OBJECT_END.length - i;
                break;
            }
        }

        for (int i = numBytes; i > 0; i--)
        {
            // todo
            // I think this might be right,
            // but it seems tricky
            assert
                bytes[bytes.length - i]
                ==
                OBJECT_END[i - 1]
                ;
        }

        return numBytes;
    }

    NonBlockingHashMap<Long, long[]> sequenceNumberToOffsets =
            new NonBlockingHashMap<>();

    public byte[] getObject(long objectSequenceNumber) throws IOException, InterruptedException
    {
        RandomAccessFile in = new RandomAccessFile(file.toFile(), "r");

        long[] offsets = sequenceNumberToOffsets.get(objectSequenceNumber);

        if (offsets == null)
        {
            //I hate this boxing, does trove have a list?
            ArrayList<Long> offsetList = new ArrayList<>();

            int arrayLength = (int) (in.length() / PIPE_BUF);

            for (int i = 0; i < arrayLength; i++)
            {
                long sequenceNumber = sequenceArray.get(i);
                // todo this means I have to treat index 0 specially or not use it
                if (sequenceNumber < 0)
                {
                    in.seek(i * PIPE_BUF);
                    sequenceNumber = sequenceNumberOfPage(in);
                    sequenceArray.set(i, sequenceNumber);
                }

                // todo bit of a shame to waste the calculation for other sequence
                // number offset relationships, but I'd really rather not memoize with
                // ArrayLists
                if (sequenceNumber != objectSequenceNumber)
                {
                    continue;
                }

                offsetList.add((in.getFilePointer() / PIPE_BUF) * PIPE_BUF);
            }

            offsets = new long[offsetList.size()];
            int index = 0;
            for (long i : offsetList)
            {
                offsets[index] = i;
            }

            if (offsets.length == 0)
            {
                throw new IOException("Tried to read an object that doesn't exist yet");
            }

            sequenceNumberToOffsets.put(objectSequenceNumber, offsets);
        }

        if (offsets.length == 0)
        {
            throw new IOException("tried to get an object that hasn't been written yet");
        }

        in.seek(offsets[0]);

        // todo I know it's terrible. we have all the offsets and then
        // just use one, I didn't know we would have all this
        // information when I wrote the other function, will revise
        List<byte[]> pages = pageListForNextObject(in);

        int totalLength = pages.stream().mapToInt(page -> page.length).sum();

        byte[] serializedObject = new byte[totalLength];
        int readBufferHead = 0;

        for (byte[] page : pages)
        {
            System.arraycopy(page, 0, serializedObject, readBufferHead, page.length);
            readBufferHead += page.length;
        }

        return serializedObject;
    }


    long sequenceNumberOfPage(RandomAccessFile in) throws IOException, InterruptedException
    {
        long position = in.getFilePointer();
        assert position % PIPE_BUF == 0;

        in.seek(position + PAGE_BEGIN.length);

        long objectSequenceNumber = in.readLong();

        sequenceArray.set((int) position / PIPE_BUF, objectSequenceNumber);

        return objectSequenceNumber;
    }

    long offsetForNextObject(List<Long> offsetsForPreviousObject, RandomAccessFile in) throws IOException
    {
        // shall include the first page after the previous object pages but no more after that
        List<Long> candidates = new ArrayList<>();
        for (int i = 1; i < offsetsForPreviousObject.size(); i++)
        {
            for
            (
                long difference = offsetsForPreviousObject.get(i-1) - offsetsForPreviousObject.get(i);
                difference != PIPE_BUF;
                difference += PIPE_BUF
            )
            {
                candidates.add((offsetsForPreviousObject.get(i) - difference) + PIPE_BUF);
            }
        }
        candidates.add(offsetsForPreviousObject.get(offsetsForPreviousObject.size()-1) + PIPE_BUF);

        byte[] candidateBytes = new byte[OBJECT_BEGIN.length];

        for (int i = 0; i < candidates.size() - 1; i++)
        {
            in.seek(candidates.get(i) + PAGE_BEGIN.length + Long.BYTES);
            in.read(candidateBytes);
            if (Arrays.equals(candidateBytes, OBJECT_BEGIN))
            {
                return candidates.get(i);
            }
        }

        while (in.getFilePointer() < in.length())
        {

        }

        //bogus return so I can run an unrelated test
        return 1;
    }


    //todo need to position in at the beginning of the next object,
    //which-- due to our fancy atomic append scheme-- might be the very next page
    private List<byte[]> pageListForNextObject(RandomAccessFile in) throws IOException
    {
        List<byte[]> delimited = allPagesForNextEntry(in);

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


    public void validateAndWriteDelimitedPage(Path file, byte[] page) throws IOException
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

        long retVal = appendToFileHelper(page);

        if (retVal < 0 || retVal < page.length)
        {
            throw new IOException("The page was " + page.length + " bytes long but write returned " + retVal);
        }
    }

    // So this is not great for caching, not a great design long term, but
    // short term it's super easy to grok so it feels pretty worth it for now
    byte[] delimitedPage(long sequenceNumber)
    {
        byte[] page = new byte[PIPE_BUF];
        System.arraycopy(PAGE_BEGIN, 0, page, 0, PAGE_BEGIN.length);
        ByteBuffer.wrap(page, 4, 8).putLong(sequenceNumber);

        System
          .arraycopy
          (
            PAGE_END,
            0,
            page,
            PIPE_BUF - PAGE_END.length - 1,
            PAGE_END.length
          );

        return page;
    }

    //ok, let's try writing a new version of this while I'm like mostly sober
    public List<byte[]> delimitedObject(byte[] serializedObject)
    {
        long sequenceNumber = getObjectCount(file).getAndIncrement();
        int numPages =
            numberOfPagesForSerializedObject(serializedObject.length);
        int objectPosition = 0;
        List<byte[]> delimitedPages = new ArrayList<>(numPages);

        for (int i = 0; i < numPages; i++)
        {
            byte[] page = delimitedPage(sequenceNumber);
            // two ints = 8 plus another 8 for the sequence number
            int availableSpace = PIPE_BUF - 16;
            // start after PAGE_BEGIN and sequence number
            int pagePosition = 12;

            if (i == 0) // first page needs OBJECT_BEGIN
            {
                System
                    .arraycopy
                    (
                        OBJECT_BEGIN,
                        0,
                        page,
                        pagePosition,
                        OBJECT_BEGIN.length
                    );
                pagePosition += OBJECT_BEGIN.length;
                availableSpace -= OBJECT_BEGIN.length;
            }


            System.out.println("params are: serializedObject: "+ serializedObject +", \n" +
                               "objectPosition: "+ objectPosition +", \n" +
                               "page,: "+ page +" \n" +
                               "pagePosition: "+ pagePosition +", \n" +
                               "availableSpace: "+ availableSpace );

            int remainingObjectLength = serializedObject.length - objectPosition;

            int amountToWrite = Math.min(availableSpace, remainingObjectLength);

            System
                .arraycopy
                (
                    serializedObject,
                    objectPosition,
                    page,
                    pagePosition,
                    amountToWrite
                );

            objectPosition += amountToWrite;
            pagePosition += amountToWrite;

            if (i == numPages - 1) //last page needs OBJECT_END
            {
                // so I think the only sensible way to deal with this is to
                // allow partial or full overwrites of PAGE_END by OBJECT_END,
                // that way we can still have sensible page parsing and we
                // still have recognizable bytes at the end of each object and
                // page so we can ensure pages are read entirely and still
                // recognize the end of object properly


                System
                    .arraycopy
                    (
                        OBJECT_END,
                        0,
                        page,
                        pagePosition,
                        OBJECT_END.length
                    );
            }

            delimitedPages.add(i, page);
        }

        return delimitedPages;
    }



    private static final int PAGE_BEGIN_INT = ByteBuffer.wrap(PAGE_BEGIN).getInt();
    private static final int OBJECT_BEGIN_INT = ByteBuffer.wrap(OBJECT_BEGIN).getInt();
    private static final int PAGE_END_INT = ByteBuffer.wrap(PAGE_END).getInt();
    private static final int OBJECT_END_INT = ByteBuffer.wrap(OBJECT_END).getInt();

    //readObject(long objectSerialNumber, (byte[] object) -> )
    //writeObject(byte[] object(or byte[] producing interface), (long objectSequenceNumber) ->


    //this should be the interface for this class,
    // but how to do the threading without using threadpools?
    //it seems like if I want to make this a library I wouldn't spawn
    // up a bunch of threads myself. Like maybe a merge thread, like probably
    // a merge thread

    //ok, so still yes on merge thread, that's probably going to be a hard requirement
    //but maybe let's not do the callback thing here. That's something other people
    // (including future me) can do themselves since they have a better idea what
    // their threading requirements are going to be.
    //The method signatures should therefore be:
    // readObject(long objectSerialNumber, Path file, byte[] object)
    // which reads the object into the byte[] named object
    // long writeObject(byte[] object, Path file)
    // which returns object serial number that was assigned to this object




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

        boolean fail = ByteBuffer.wrap(nextPage, 12, 4).getInt() == OBJECT_BEGIN_INT;

        assert fail;
        assert ByteBuffer.wrap(nextPage, (PIPE_BUF - 4), 4).getInt() == PAGE_END_INT;

        long objectCountNumber = ByteBuffer.wrap(nextPage, 4, 8).getLong();

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
                System.out.println(("Exhausted InputStream"));
                break;
            }

            assert PAGE_BEGIN_INT == ByteBuffer.wrap(nextPage, 0, 4).getInt();

            //todo assert page ends with PAGE_END

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

            //TODO this is a problem, need to handle the wraparound case
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
