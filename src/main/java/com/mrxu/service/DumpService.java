package com.mrxu.service;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.mrxu.datasource.DataSourceConfig;
import com.mrxu.disruptor.event.DataEvent;
import com.mrxu.disruptor.factory.DataEventFactory;
import com.mrxu.disruptor.handler.DataEventHandler;
import com.mrxu.disruptor.producer.DataEventProducer;
import com.mrxu.mysql.Connect;
import com.mrxu.mysql.ConnectConfig;
import com.mrxu.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.util.List;
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
        //导入数据的源库
        DataSourceConfig sourceDataSource = new DataSourceConfig(sourceConfig);
        //导入数据的目标库
        DataSourceConfig targetDataSource = new DataSourceConfig(targetConfig);
        //获取disruptor队列
        Disruptor<DataEvent> disruptor = initDisruptor(targetDataSource);
        //开始job
        loadDataJob(disruptor, sourceDataSource, targetDataSource);
    }


    private void loadDataJob(Disruptor<DataEvent> disruptor, DataSourceConfig sourceDataSource, DataSourceConfig targetDataSource) {
        disruptor.start();
        RingBuffer<DataEvent> ringBuffer = disruptor.getRingBuffer();
        //生产者
        DataEventProducer producer = new DataEventProducer(ringBuffer);
        //初始化需要同步数据库的信息
        LoadDataFromDatabase loadData = new LoadDataFromDatabase(sourceDataSource);
        Long maxId = loadData.getMaxId();
        Long minId = loadData.getMinId();
        Integer batchSize = loadData.getBatchSize();
        int batch = (int) Math.ceil((double) (maxId - minId) / (double) batchSize);
        CountDownLatch latch = new CountDownLatch(batch);
        for (long i = 1; i <= batch; i++) {
            long finalI = i;
            JobService.submit(() -> job(producer, loadData, targetDataSource.getConfig(), finalI, batchSize, latch));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("InterruptedException ", e);
        }
        disruptor.shutdown();
        sourceDataSource.getDataSource().close();
        targetDataSource.getDataSource().close();
    }

    private void job(DataEventProducer producer, LoadDataFromDatabase loadData, ConnectConfig targetConfig, long i, int batchSize, CountDownLatch latch) {
        try {
            ResultSet resultSet = loadData.load((i - 1) * batchSize, i * batchSize);
            List<String> fields = targetConfig.getFields();
            while (resultSet.next()) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < fields.size(); j++) {
                    Object object = resultSet.getObject(fields.get(j));
                    sb.append(object);
                    if (j != fields.size() - 1) {
                        sb.append("\t");
                    }
                }
                producer.onData(sb.toString());
            }
        } catch (Exception e) {
            log.error("Exception while loading data, error message: ", e);
        } finally {
            latch.countDown();
        }
    }

    private Disruptor<DataEvent> initDisruptor(DataSourceConfig targetDataSource) {
        DataEventFactory factory = new DataEventFactory();
        Disruptor<DataEvent> disruptor = new Disruptor<>(factory, Utils.getBatchSize(targetDataSource.getConfig().getBatchSize()), Executors.defaultThreadFactory());
        disruptor.handleEventsWith(new DataEventHandler(targetDataSource));
        return disruptor;
    }

//    public static void main(String[] args) {
//        for (int i = 0; i < 100; i++) {
//            service.submit(() -> {
//                System.out.println("hello");
//                try {
//                    TimeUnit.SECONDS.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
//    }

//    public static void main(String[] args) {
//        Long maxId = 101L;
//        Long minId =  0L;
//        Integer batchSize = 10;
//
//        int batch = (int) Math.ceil((double) (maxId - minId) / (double) batchSize);
//        for (long i = 1; i <= batch; i++) {
//            System.out.println(i * batchSize);
//        }
//    }

}
