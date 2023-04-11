package io.openmessaging.test;

import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.common.Define;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;

/**
 * 测试程序
 * @author chender
 * @date 2021/9/11 14:37
 */
public class Tester {
    public static void main(String[] args)throws Exception {
        DefaultMessageQueueImpl defaultMessageQueue=new DefaultMessageQueueImpl();
        Thread[] threads=new Thread[40];
        for(int i=0;i<40;i++){
            int fi=i;
            threads[i]=new Thread(){
                @Override
                public void run() {
                    produce(defaultMessageQueue,fi);
                    comsume(defaultMessageQueue,fi);
                }
            };
        }
        for(Thread t:threads){
            t.start();
        }
        for(Thread t:threads){
            t.join();
        }
        System.out.println("cost:"+(System.currentTimeMillis()-Define.startTime));
        System.exit(-1);
    }

    public static void produce(DefaultMessageQueueImpl defaultMessageQueue,int idx){
        ByteBuffer byteBuffer=ByteBuffer.allocate(17*1024);
        Random random=new Random(idx);
        String topic="topic"+idx;
        for(int i=0;i<1650;i++){
            byteBuffer.clear();
            byteBuffer.putInt(idx).position(0);
            byteBuffer.limit(random.nextInt(Define.maxDataLength-100)+100);
            defaultMessageQueue.append(topic,0,byteBuffer);
        }
    }

    public static void comsume(DefaultMessageQueueImpl defaultMessageQueue,int idx){
        Random random=new Random(idx);
        String topic="topic"+idx;
        for(int j=0;j<16;j++){
            Map<Integer,ByteBuffer> result=defaultMessageQueue.getRange(topic,0,j*100,100);
            for(int i=0;i<100;i++){
                ByteBuffer byteBuffer=result.get(i);
                int rIdx=byteBuffer.getInt();
                if(rIdx!=idx){
                    System.out.println(rIdx+"!="+idx);
                }
                if(byteBuffer.limit()!=random.nextInt(Define.maxDataLength-100)+100){
                    System.out.println("fail");
                }
            }
        }
    }
}
