package org.josh.JoshDB;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;

public class MessageHandlingPipeline
{
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

    //I'm not sure MessageAndCodeBundle is the right abstraction here
    private final Disruptor<MessageAndCodeBundle> disruptor;


    public MessageHandlingPipeline()
    {
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
            (EventHandler<MessageAndCodeBundle>) (event, sequence, endOfBatch) ->
            {
                return;
            }
          );
        //noinspection unchecked
        disruptor
          .handleEventsWith
          (
            (MessageAndCodeBundle bundle, long l, boolean b) ->
            {
            }
          )
          .then
          (
            (MessageAndCodeBundle bundle, long l, boolean b) ->
            {
            }
          );

        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }
}
