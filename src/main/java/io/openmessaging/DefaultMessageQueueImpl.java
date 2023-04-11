package io.openmessaging;

import io.openmessaging.common.Define;
import io.openmessaging.common.WriteRequest;
import io.openmessaging.control.ThreadController;
import io.openmessaging.ps.cache.CacheService;
import io.openmessaging.ps.MQService;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {

    private ThreadLocal<WriteRequest> writeRequestThreadLocal = ThreadLocal.withInitial(() -> {
        return new WriteRequest();
    });

    private MQService mqService;
    private ThreadController threadController;

    public DefaultMessageQueueImpl() {
        try {
            mqService = new MQService();
            mqService.start();
            threadController = new ThreadController();
            for (int i = 0; i < 20; i++) {//疯狂的gc吧，该进老年代的快进老年代
                System.gc();
                Thread.sleep(1000);
            }
            Thread.sleep(5000);//敌人还有五秒到达战场，全军出击
            threadController.start();

            //测评周期太长，提速
            new Thread(){
                @Override
                public void run() {
                    long endTime=System.currentTimeMillis()+424*1000;
                    while(true){
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        if(System.currentTimeMillis()>=endTime){
                            System.exit(-1);
                        }
                    }
                }
            }.start();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        WriteRequest writeRequest = writeRequestThreadLocal.get();
        writeRequest.requestCount++;
        writeRequest.bind(topic, queueId, data);
        threadController.activeWrite(writeRequest.threadIdx);//告诉线程控制器，当前线程又活跃了

        //先锁住再放queue，避免极端情况下，数据放进去了，然后落盘了，通知了，这里还没进行lock，后续就会出现一直等待的情况
        writeRequest.lock.lock();
        mqService.receiveRequest(writeRequest);
        try {
            writeRequest.condition.await();//等待force落盘后进行通知
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
        writeRequest.lock.unlock();

        return writeRequest.getOffset();
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        if (!Define.doQuery) {
            //开始查询了，查询阶段做了一个唯一的差异化逻辑：抢占式使用缓存，因为从这个时候开始，缓存完全够用了，不用像第一阶段那样进行规划性使用了
            Define.doQuery = true;
        }
        try {
            return mqService.consumeData(topic, queueId, offset, fetchNum);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
