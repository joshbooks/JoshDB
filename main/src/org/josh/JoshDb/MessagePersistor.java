package org.josh.JoshDb;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;

public class MessagePersistor {
    private final RingBuffer<MessageAndCodeBundle> ringBuffer;
    private static final Logger logger = LoggerFactory.getLogger(MessagePersistor.class);

    private final ArrayList<Thread> disruptorThreads = new ArrayList<>();
    ThreadFactory disruptorThreadFactory =
        (runnable) ->
        {
            Thread disruptorThread = new Thread(runnable);
            disruptorThreads.add(disruptorThread);
            return disruptorThread;
        };

    private int bufferSize = 1024;
    // so clearly it isn't ok that things just get written to a buffer
    // and are then considered "persisted" ideally what we want to do
    // is write a batch of messages to the buffer, flush the buffer, then
    // execute the code associated with te messages in that batch. There is
    // at least one way to do this super awesomefully with the disruptor
    //but we're going to save that for later
    private final ArcCloseable<BufferedWriter> logFileWriter;

    // so I think that the MessageAndCode bundle, as well as it might work right now
    // is actually a bad idea, in the future it's going to make more sense for the
    // message and code to be produced by separate producers and stored independently
    // of each other since in real life it will be effectively two "producers" doing
    // the producing of those two pieces of information, like a Web Server getting
    // a post request with the message, then it should pretty much always be persisted
    // but the code to be executed after the message is persisted must be determined
    // before the message is written but requires reading the message and potentially
    // other more expensive operations, like reading the hard disk

    // So I think the org.josh.JoshDb.Node code should really just be a switch on the message type
    // or better yet a HashMap lookup on the message type for the code to execute
    // afterwards. The MessageAndCode bundle stays

    private final Disruptor<MessageAndCodeBundle> disruptor;

    public MessagePersistor(Path persistDestination) throws IOException {
        //todo this is no bueno, dealing with flushing and shit is going to be a
        //huge problem and I don't like it. I don't like it at all. But I have to
        // deal with it somehow
        logFileWriter = ArcCloseable.arcWriterForFile(persistDestination);
        logFileWriter.incRef();


        disruptor =
                new Disruptor<>
                (
                    MessageAndCodeBundle::new,
                    bufferSize,
                        disruptorThreadFactory
                );

        //noinspection unchecked
        disruptor
            .handleEventsWith
            (
                (MessageAndCodeBundle bundle, long l, boolean b) ->
                {
                    if (!logFileWriter.incRef())
                    {
                        logger.error("This not good");
                    }
                    logFileWriter.getBacking().write(bundle.msg.toString());
                    logFileWriter.getBacking().flush();
                    logFileWriter.decRef();
                }
            )
            .then
            (
                (MessageAndCodeBundle bundle, long l, boolean b) ->
                    bundle.code.onMessagePersisted(bundle.msg)
            );

        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    public void persistMessage(Message msg, MessageCallback callback)
    {
        persistMessage(new MessageAndCodeBundle(msg, callback));
    }

    public void persistMessage(MessageAndCodeBundle bundle)
    {
        ringBuffer.publishEvent((event, sequence) ->
        {
            event.code = bundle.code;
            event.msg = bundle.msg;
        });
    }

    public void shutDown() throws IOException
    {
        disruptor.shutdown();
        logFileWriter.decRef();

        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            //hope everything is ok
        }

        disruptorThreads.forEach(Thread::interrupt);
    }
}
