package org.josh.JoshDB;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;

public class DoesDisruptorWorkLikeThis
{
    static RingBuffer<StringBuilder> ringBuffer;
    Logger logger = LoggerFactory.getLogger(MessagePersistor.class);

    static ArrayList<Thread> disruptorThreads = new ArrayList<>();
    static ThreadFactory disruptorThreadFactory =
            (runnable) ->
            {
                Thread disruptorThread = new Thread(runnable);
                disruptorThreads.add(disruptorThread);
                return disruptorThread;
            };

    static int bufferSize = 1024;

    //I'mnot sure MessageAndCodeBundle is the right abstraction here
    static Disruptor<StringBuilder> disruptor;

    public static void main(String[] args)
    {
        disruptor =
                new Disruptor<>
                        (
                                StringBuilder::new,
                                bufferSize,
                                disruptorThreadFactory
                        );

        disruptor.handleEventsWith((event, sequence, endOfBatch) ->
        {
            System.out.println("First stage: "+event.toString());
            event.append(" hi");
        })
        .then((event, sequence, endOfBatch) ->
                {
                    System.out.println("Second stage: "+event.toString());
                }
        ).then((event, sequence, endOfBatch) -> event = null);


        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();

        ringBuffer.publishEvent(((event, sequence) ->
                                 {
                                     event.append("hello");

        }));
        //TODO aw shit, this changes everything, I think I know how to do
        // MessageHandlingPipeline and I think MessageAndCodeBundle will
        // still work
    }
}
