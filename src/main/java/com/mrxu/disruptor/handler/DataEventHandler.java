package com.mrxu.disruptor.handler;

import com.lmax.disruptor.EventHandler;
import com.mrxu.datasource.DataSourceConfig;
import com.mrxu.disruptor.event.DataEvent;
import com.mrxu.service.DumpDataToDatabase;
import com.mrxu.util.Utils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 14:16
 */
@Slf4j
public class DataEventHandler implements EventHandler<DataEvent> {

    private StringBuffer cache;

    private DumpDataToDatabase dumpData;

    private AtomicLong total;
    private AtomicLong success;

    public DataEventHandler(DataSourceConfig targetDataSource) {
        this.dumpData = new DumpDataToDatabase(targetDataSource);
        this.total = new AtomicLong(0);
        this.success = new AtomicLong(0);
        this.cache = new StringBuffer();
    }

    @Override
    public void onEvent(DataEvent dataEvent, long sequence, boolean endOfBatch) throws Exception {
        long cnt = total.incrementAndGet();
        //向cache中添加数据
        cache.append(dataEvent.getValue());
        if (endOfBatch) {
            int i = dumpData.loadLocalFile(Utils.getDataStream(cache));
            long s = success.addAndGet(i);
            cache = new StringBuffer();
            log.info("load data total: " + cnt);
            log.info("load data success: " + s);
            return;
        }
        //一批数据的最后一条不需要换行
        cache.append("\n");
    }
}
