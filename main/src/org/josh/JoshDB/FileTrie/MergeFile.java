package org.josh.JoshDB.FileTrie;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


public class MergeFile
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
  // objectSequenceNumber.getAndIncrement()) followed by a long containing the
  // remaining serialized object data (this means that we know for sure when
  // we're at the last page, and if so how much of the last page contains actual data,
  // plus if we decide to use UDP then we have ordering built in.
  // This is followed by OBJECT_BEGIN and then data, ending with OBJECT_END
  // once we've written out all the data in the object, whether or not we'e
  // filled up the entire page.
  // If OBJECT_END occurs before the end of the page, the remainder of the page
  // must be filled with zeroes up to PAGE_END if object end occurs less than 4 bytes
  // before PAGE_END it shall overwrite the bytes of PAGE_END

  // hacky nonsense, should be dynamically determined
  // based on the underlying filesystem, but using ext3 for now
  // because that's what my laptop runs
  public static final int PIPE_BUF = 4096;

  public static final byte[] OBJECT_BEGIN = new byte[]{'f', 'u', 'c', 'k'};

  public static final byte[] PAGE_BEGIN = new byte[]{'d', 'a', 'm', 'n'};

  public static final byte[] OBJECT_END = new byte[]{'s', 'h', 'i', 't'};

  public static final byte[] PAGE_END = new byte[]{'z', '/', 'o', 's'};

  // Where does the PAGE_BEGIN go?
  public static final int PAGE_BEGIN_POS = 0;
  public static final int PAGE_BEGIN_END_POS = PAGE_BEGIN_POS + PAGE_BEGIN.length;
  public static final int PAGE_BEGIN_LENGTH = PAGE_BEGIN.length;

  // Where does the sequence number go?
  public static final int SEQUENCE_NUMBER_POS = PAGE_BEGIN_END_POS;
  public static final int SEQUENCE_NUMBER_END_POS = SEQUENCE_NUMBER_POS + Long.BYTES;
  public static final int SEQUENCE_NUMBER_LENGTH = Long.BYTES;

  // Where does the amount remaining go?
  public static final int AMOUNT_REMAINING_POS = SEQUENCE_NUMBER_END_POS;
  public static final int AMOUNT_REMAINING_END_POS = AMOUNT_REMAINING_POS + Integer.BYTES;
  public static final int AMOUNT_REMAINING_LENGTH = Integer.BYTES;

  // Where does the OBJECT_BEGIN go?
  public static final int OBJECT_BEGIN_POS = AMOUNT_REMAINING_END_POS;
  public static final int OBJECT_BEGIN_END_POS = OBJECT_BEGIN_POS + OBJECT_BEGIN.length;
  public static final int OBJECT_BEGIN_LENGTH = OBJECT_BEGIN.length;

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
//      System.out.println("starting with fd " + fd.get());
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
  // todo we don't do iterating anymore, that changes some things
  private static NonBlockingHashMap<Path, MergeFile> registry =
          new NonBlockingHashMap<>();

  // So I think the ultimate idea is to have a thread pool doing all the IO
  // so what we want is a worker thread function fed off someting like a queue
  // (disruptor anyone?) and any thread may write to any file at any time
  // and come to think about it if we have one big disruptor for all the stages
  // of the databases (write ahead?, confirm write ahead by other nodes?,
  // perform update in memory?, tell other nodes what the result is?, return result?),
  // this whole thing can be kept largely copy free
  // (after I optimize the crap out of the code in this file of course [sys/uio.h FTW!])

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

    NonBlockingHashMap
    <
      Long,
      AtomicReference<PageInfo.BasicPageInfo[]>
    >
    sequenceNumberToPageInfoList =
      new NonBlockingHashMap<>();

    boolean addPageInfo(byte[] page, long offset)
    {
      long sequenceNumber = sequenceNumberOfPage(page);

      PageInfo.BasicPageInfo pageInfo =
        PageInfo.basicInfoFromPage(page, offset);

      AtomicReference<PageInfo.BasicPageInfo[]> pageInfoArrAtomRef =
        new AtomicReference<>(null);

      AtomicReference<PageInfo.BasicPageInfo[]> temp =
        sequenceNumberToPageInfoList
          .putIfAbsent(sequenceNumber, pageInfoArrAtomRef);

      PageInfo.BasicPageInfo[] singletonPageInfoArr =
        new PageInfo.BasicPageInfo[]{ pageInfo };

      if (temp != null)
      {
        pageInfoArrAtomRef = temp;
      }

      while (pageInfoArrAtomRef.get() == null)
      {
        if
        (
          pageInfoArrAtomRef
            .compareAndSet
            (
              null,
              singletonPageInfoArr
            )
        )
        {
          return true;
        }
      }

      PageInfo.BasicPageInfo[] currentPageInfoArr;
      PageInfo.BasicPageInfo[] mergedArr;

      do
      {
        currentPageInfoArr = pageInfoArrAtomRef.get();
         mergedArr =
          mergeArrs
          (
            currentPageInfoArr,
            singletonPageInfoArr
          );

         if (currentPageInfoArr.length == mergedArr.length)
         {
           return false;
         }
      } while (!pageInfoArrAtomRef.compareAndSet(currentPageInfoArr, mergedArr));

      return true;
    }

  private PageInfo.BasicPageInfo[] mergeArrs(PageInfo.BasicPageInfo[] arrOne, PageInfo.BasicPageInfo[] arrTwo)
  {
    // iterate through both to find length of merged arr (weed out dups)
    int arrOneIt = 0;
    int arrTwoIt = 0;
    int uniqueLength = 0;
    for
    (
      int i = arrOneIt + arrTwoIt;
      i < arrOne.length + arrTwo.length;
      i = arrOneIt + arrTwoIt
    )
    {
      if (arrOneIt == arrOne.length)
      {
        arrTwoIt++;
        uniqueLength++;
      }
      else if (arrTwoIt == arrTwo.length)
      {
        arrOneIt++;
        uniqueLength++;
      }
      else if (arrOne[arrOneIt].amountRemaining == arrTwo[arrTwoIt].amountRemaining)
      {
        // discard from arrTwo when there is a dup
        arrTwoIt++;
      }
      else if (arrOne[arrOneIt].amountRemaining > arrTwo[arrTwoIt].amountRemaining)
      {
        arrOneIt++;
        uniqueLength++;
      }
      else
      {
        arrTwoIt++;
        uniqueLength++;
      }
    }

    PageInfo.BasicPageInfo[] mergedArr = new PageInfo.BasicPageInfo[uniqueLength];

    // iterate through both arrays to do merge
    arrOneIt = 0;
    arrTwoIt = 0;
    int mergePos = 0;
    for
    (
      int i = arrOneIt + arrTwoIt;
      i < arrOne.length + arrTwo.length;
      i = arrOneIt + arrTwoIt
    )
    {
      if (arrOneIt == arrOne.length)
      {
        mergedArr[mergePos++] = arrTwo[arrTwoIt++];
      }
      else if (arrTwoIt == arrTwo.length)
      {
        mergedArr[mergePos++] = arrOne[arrOneIt++];
      }
      else if (arrOne[arrOneIt].amountRemaining == arrTwo[arrTwoIt].amountRemaining)
      {
        // discard from arrTwo when there is a dup
        arrTwoIt++;
      }
      else if (arrOne[arrOneIt].amountRemaining > arrTwo[arrTwoIt].amountRemaining)
      {
        mergedArr[mergePos++] = arrOne[arrOneIt++];
      }
      else
      {
        mergedArr[mergePos++] = arrTwo[arrTwoIt++];
      }
    }

    return mergedArr;
  }


  static int numberOfPagesForSerializedObject(int length)
    {
      // long is for sequence number, integer is for remaining length
      int usabePageLength =
        PIPE_BUF - PAGE_END.length
        - PAGE_BEGIN.length - Long.BYTES - Integer.BYTES;

      length += OBJECT_BEGIN.length + OBJECT_END.length;

      int numPages = length / usabePageLength;
      numPages += length % usabePageLength != 0 ? 1 : 0;

//      System.out.println("Expected number of pages for " + length + " is " + numPages);

      return numPages;
    }

    static int amountRemainingForPage(byte[] page)
    {
      assert page.length == PIPE_BUF;
      return ByteBuffer.wrap(page, AMOUNT_REMAINING_POS, AMOUNT_REMAINING_LENGTH).getInt();
    }

    static long sequenceNumberOfPage(byte[] page)
    {
        assert page.length == PIPE_BUF;
        return ByteBuffer.wrap(page, SEQUENCE_NUMBER_POS, SEQUENCE_NUMBER_LENGTH).getLong();
    }


    public static final NonBlockingHashMap<Path, AtomicLong> objectCountForFile =
            new NonBlockingHashMap<>();

    /**
     * Every time you want to write an object to a MergeFile, you just
     * getObjectCount and then
     * getAndAdd that guy and write out the resulting long after each
     * PAGE_BEGIN marker for that object, that way we can keep track
     * of which Object each page corresponds to
     */
    static AtomicLong getObjectCount(Path file)
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

    // OBJECT_END should just be a check. In the metadata we store in an object
    // we should store object length and use that to determine where the end is
    static byte[] undelimitedObject(List<byte[]> delimitedPages)
    {
        byte[] object = null;
        long objectSequenceNumber = -1;
        int objectPosition = 0;

        // todo ok, so where do we expect the OBJECT_END bytes to be?
        for (int i = 0; i < delimitedPages.size(); i++)
        {
            byte[] delimitedPage = delimitedPages.get(i);
            // TODO this should be a global constant whose calculation is well commented
            int objectDataBegin = 16;
            // TODO same here
            int dataInPage = PIPE_BUF - objectDataBegin - PAGE_END.length;

            // extract metadata
            long pageSequenceNumber = ByteBuffer.wrap(delimitedPage, SEQUENCE_NUMBER_POS, SEQUENCE_NUMBER_LENGTH).getLong();
            int remainingLength = ByteBuffer.wrap(delimitedPage, AMOUNT_REMAINING_POS, AMOUNT_REMAINING_LENGTH).getInt();

            if (objectSequenceNumber == -1)
            {
              objectSequenceNumber = pageSequenceNumber;
            }
            else
            {
              assert objectSequenceNumber == pageSequenceNumber;
            }

            if (i == 0)
            {
              //todo assert OBJECT_BEGIN is there
              // copy the data after OBJECT_BEGIN
              objectDataBegin += OBJECT_BEGIN.length;
              dataInPage -= OBJECT_BEGIN.length;
              object = new byte[remainingLength];
            }
            // not mutually exclusive with this being the first page
            if (i == delimitedPages.size() - 1)
            {
              // make sure OBJECT_END is where we expect it to be and copy all
              // the data before that
              // todo assert OBJECT_END is there
              dataInPage = remainingLength;
            }


            System.arraycopy(delimitedPage, objectDataBegin, object, objectPosition, dataInPage);
            objectPosition += dataInPage;
        }


        return object;
    }


    static boolean isMemberOfObject(byte[] page, long sequenceNumber)
    {
        assert
            ByteBuffer.wrap(page, PAGE_BEGIN_POS, PAGE_BEGIN_LENGTH).getInt()
            ==
            PAGE_BEGIN_INT;

        return ByteBuffer.wrap(page, SEQUENCE_NUMBER_POS, SEQUENCE_NUMBER_LENGTH).getLong() == sequenceNumber;
    }

    // So this is not great for caching, not a great design long term, but
    // short term it's super easy to grok so it feels pretty worth it for now
    static byte[] delimitedPage(long sequenceNumber, int remainingLength)
    {
        byte[] page = new byte[PIPE_BUF];
        System.arraycopy(PAGE_BEGIN, PAGE_BEGIN_POS, page, 0, PAGE_BEGIN.length);
        ByteBuffer.wrap(page, SEQUENCE_NUMBER_POS, SEQUENCE_NUMBER_LENGTH).putLong(sequenceNumber);
        ByteBuffer.wrap(page, AMOUNT_REMAINING_POS, AMOUNT_REMAINING_LENGTH).putInt(remainingLength);

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

    ThreadLocal<SeekableByteChannel> byteChannelForThread = new ThreadLocal<>();

    SeekableByteChannel getByteChannel() throws IOException
    {
      SeekableByteChannel existing = byteChannelForThread.get();

      if (existing == null)
      {
        existing = Files.newByteChannel(file);
        byteChannelForThread.set(existing);
      }

      return existing;
    }

    // this method is twice as stupid as nextPage as the type it returns must be null checked
    // and it removes context by obscuring the type of exception
    // thrown and doing what it thinks is a sensible default, to do this in private code
    // is reasonable but to do it in an interface is dogma.
    byte[] nextPageRetNullOnError()
    {
      try
      {
        return nextPage();
      }
      catch (IOException e)
      {
        log.error("Encountered error: " + e.getMessage());
        e.printStackTrace();
        return null;
      }
    }

    // this is a really stupid method, it gives you no control over
    // allocation and no idea what you're reading but it seems like
    // we ought to start out doing stupid things that work well
    // rather than smart things that don't
    byte[] nextPage() throws IOException
    {
      byte[] buffer = new byte[PIPE_BUF];

      int retVal = readPage(getByteChannel(), buffer);

      if (retVal < 0)
      {
        throw new IOException("Read returned a negative value, that seems like a bad thing");
      }
      else if (retVal != PIPE_BUF)
      {
        throw new  IOException("Did not read " + PIPE_BUF + " from file. That makes no sense");
      }

      return buffer;
    }


    int readPage(SeekableByteChannel fileStream, byte[] buffer) throws IOException
    {
      assert buffer.length == PIPE_BUF;

      long pageOffset = fileStream.position();

      int retVal = fileStream.read(ByteBuffer.wrap(buffer));

      // might want to move some error handling in here
      // to get better assurances about validity of page info
      addPageInfo(buffer, pageOffset);

      return retVal;
    }

    //ok, let's try writing a new version of this while I'm like mostly sober
    static public List<byte[]> delimitedObject(byte[] serializedObject, long sequenceNumber)
    {
      int numPages =
          numberOfPagesForSerializedObject(serializedObject.length);
      int objectPosition = 0;
      List<byte[]> delimitedPages = new ArrayList<>(numPages);
      int remainingLength = serializedObject.length;

      for (int i = 0; i < numPages; i++)
      {
        byte[] page = delimitedPage(sequenceNumber, remainingLength);
        // two ints = 8 plus another 8 for the sequence number,
        // plus 4 for the remaining length
        int availableSpace = PIPE_BUF - 20;
        // start after PAGE_BEGIN, sequence number, and remaining length
        int pagePosition = 16;

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


//        System.out.println("params are: serializedObject: "+ serializedObject +", \n" +
//                           "objectPosition: "+ objectPosition +", \n" +
//                           "page,: "+ page +" \n" +
//                           "pagePosition: "+ pagePosition +", \n" +
//                           "availableSpace: "+ availableSpace );

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
        remainingLength -= amountToWrite;

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

      assert delimitedPages.size() == numberOfPagesForSerializedObject(serializedObject.length);
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
