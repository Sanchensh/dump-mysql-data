package com.mrxu.disruptor.factory;

import com.lmax.disruptor.EventFactory;
import com.mrxu.disruptor.event.DataEvent;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 14:15
 */
public class DataEventFactory implements EventFactory<DataEvent> {
    @Override
    public DataEvent newInstance() {
        return new DataEvent();
    }
}
