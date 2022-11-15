package com.mrxu.disruptor.producer;

import com.lmax.disruptor.RingBuffer;
import com.mrxu.disruptor.event.DataEvent;

import java.nio.ByteBuffer;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 14:17
 */
public class DataEventProducer {
    private final RingBuffer<DataEvent> ringBuffer;

    public DataEventProducer(RingBuffer<DataEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(String data) {
        long sequence = ringBuffer.next();
        try {
            DataEvent dataEvent = ringBuffer.get(sequence);
            dataEvent.setValue(data);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
