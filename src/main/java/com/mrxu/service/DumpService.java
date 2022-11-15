package com.mrxu.service;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.mrxu.disruptor.event.DataEvent;
import com.mrxu.disruptor.factory.DataEventFactory;
import com.mrxu.disruptor.handler.DataEventHandler;
import com.mrxu.disruptor.producer.DataEventProducer;
import com.mrxu.mysql.Connect;
import com.mrxu.mysql.ConnectConfig;
import com.mrxu.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;

/**
 * @Author jianglei.xu@amh-group.com
 * @Date 2022/11/14 15:57
 */
@Service
@Slf4j
public class DumpService {
    //同步数据的线程池，最大同步20个
    private static final ExecutorService SyncService = new ThreadPoolExecutor(10, 10, 30, TimeUnit.SECONDS, new LinkedBlockingDeque<>(10), Executors.defaultThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
    //执行同步任务线程池
    private static final ExecutorService JobService = new ThreadPoolExecutor(10, 10, 30, TimeUnit.SECONDS, new LinkedBlockingDeque<>(), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

    public void start(Connect connect) {
        SyncService.submit(() -> configAndStartJob(connect));
    }


    private void configAndStartJob(Connect connect) {
        //导入数据的源库的配置
        ConnectConfig sourceConfig = connect.getSource();
        //导入数据的目标库的配置
        ConnectConfig targetConfig = connect.getTarget();
        //获取disruptor队列
        Disruptor<DataEvent> disruptor = initDisruptor(targetConfig);
        //开始job
        loadDataJob(disruptor, sourceConfig, targetConfig);
    }


    private void loadDataJob(Disruptor<DataEvent> disruptor, ConnectConfig sourceConnectConfig, ConnectConfig targetConnectConfig) {
        disruptor.start();
        RingBuffer<DataEvent> ringBuffer = disruptor.getRingBuffer();
        //生产者
        DataEventProducer producer = new DataEventProducer(ringBuffer);
        //初始化需要同步数据库的信息
        LoadDataFromDatabase loadData = new LoadDataFromDatabase(sourceConnectConfig);
        Long maxId = loadData.getMaxId();
        Long minId = loadData.getMinId();
        Integer batchSize = loadData.getBatchSize();
        int batch = (int) Math.ceil((double) (maxId - minId) / (double) batchSize) + 1;
        for (long i = 1; i <= batch; i++) {
            loadData.load((i - 1) * batchSize, i * batchSize, targetConnectConfig, producer);
        }
        disruptor.shutdown();
    }

    private Disruptor<DataEvent> initDisruptor(ConnectConfig targetConnectConfig) {
        DataEventFactory factory = new DataEventFactory();
        Disruptor<DataEvent> disruptor = new Disruptor<>(factory, Utils.getBatchSize(targetConnectConfig.getBatchSize()), Executors.defaultThreadFactory());
        disruptor.handleEventsWith(new DataEventHandler(targetConnectConfig));
        return disruptor;
    }

}
