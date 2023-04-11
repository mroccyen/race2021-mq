package io.openmessaging.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 调用请求
 * @author chender
 * @date 2021/9/11 18:43
 */
public class WriteRequest {

    private static AtomicInteger threadSequence=new AtomicInteger();

    public int requestCount;
    public int threadIdx;
    private int queueId;
    private ByteBuffer data;
    private volatile long offset; //这个要volatile，避免线程不可见问题
    private String topic;
    public ReentrantLock lock;
    public Condition condition;

    public WriteRequest(){
        threadIdx=threadSequence.getAndIncrement();
        this.lock=new ReentrantLock();
        this.condition=this.lock.newCondition();
    }



    public void bind(String topic,int queueId,ByteBuffer data){
        this.topic=topic;
        this.queueId=queueId;
        this.data=data;
        this.offset=-1;
    }


    public int getQueueId() {
        return queueId;
    }

    public ByteBuffer getData() {
        return data;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
