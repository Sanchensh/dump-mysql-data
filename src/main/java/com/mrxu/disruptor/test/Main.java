package com.mrxu.disruptor.test;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.mrxu.disruptor.event.DataEvent;
import com.mrxu.disruptor.factory.DataEventFactory;
import com.mrxu.disruptor.handler.DataEventHandler;
import com.mrxu.disruptor.producer.DataEventProducer;

import java.util.concurrent.Executors;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 15:01
 */
public class Main {
    public static void main(String[] args) {
        DataEventFactory factory = new DataEventFactory();
        int bufferSize = 8;

        // Construct the Disruptor
        Disruptor<DataEvent> disruptor = new Disruptor<>(factory, bufferSize, Executors.defaultThreadFactory());

        // Connect the handler
        disruptor.handleEventsWith(new DataEventHandler(null));

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<DataEvent> ringBuffer = disruptor.getRingBuffer();

        DataEventProducer producer = new DataEventProducer(ringBuffer);

        for (long  i = 0; i < 10; i++) {
            producer.onData(i + "");
        }

        disruptor.shutdown();
    }
}
