package io.openmessaging.test;

import io.openmessaging.common.Define;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * 测试磁盘
 * @author chender
 * @date 2021/9/10 18:37
 */
public class DiskTester {


    public static void testSequenceWrite(){
        Executor pool= Executors.newFixedThreadPool(8);
          int threadSize=3;
          long totalLength=75L*1024*1024*1024/threadSize;
          int batchSize=48*1024;
          Thread[] threads=new Thread[threadSize];
          for(int j=0;j<threadSize;j++){
              int fj=j;
              threads[j]=new Thread(){
                  @Override
                  public void run() {
                      try {
                          File file=new File(Define.ESSDPATH+ File.separator+fj+".testData");
                          FileChannel fileChannel = FileChannel.open(file.toPath(), CREATE, WRITE,READ);
                          long start=System.currentTimeMillis();
                          ByteBuffer byteBuffer=ByteBuffer.allocateDirect(batchSize);
                          long writePosition=0;
//                          MappedByteBuffer mmap=fileChannel.map(FileChannel.MapMode.READ_WRITE,writePosition,batchSize*1024);
                          writePosition+=batchSize*1024;
                          for(long i=0;i<totalLength;i+=batchSize){
                              byteBuffer.position(0).limit(batchSize);
//                              mmap.put(byteBuffer);
//                              mmap.force();
//                              if(mmap.remaining()==0){
//                                  MappedByteBuffer tmmap=mmap;
//                                  pool.execute(()->{
//                                      ((DirectBuffer)tmmap).cleaner().clean();
//                                  });
//                                  mmap=fileChannel.map(FileChannel.MapMode.READ_WRITE,writePosition,batchSize*1024);
//                                  writePosition+=batchSize*1024;
//                              }
                              fileChannel.write(byteBuffer);
                              fileChannel.force(false);
                          }
                          System.out.println("write cost1:"+(System.currentTimeMillis()-start));
                      }catch (Exception e){
                          e.printStackTrace();
                      }
                  }
              };
          }
          for(Thread thread:threads){
              thread.start();
          }
          try {
              for(Thread thread:threads){
                  thread.join();
              }
          }catch (Exception e){
              e.printStackTrace();
          }
    }


    public static void testMMAPWrite(){
        Executor pool= Executors.newFixedThreadPool(4);
        int threadSize=4;
        long totalLength=75L*1024*1024*1024/threadSize;
        int batchSize=84*1024;
        Thread[] threads=new Thread[threadSize];
        File file=new File(Define.ESSDPATH+ File.separator+"1.testData");
        for(int j=0;j<threadSize;j++){
            int fj=j;
            threads[j]=new Thread(){
                @Override
                public void run() {
                    try {
                        FileChannel fileChannel = FileChannel.open(file.toPath(), CREATE, WRITE,READ);
                        long start=System.currentTimeMillis();
                        ByteBuffer byteBuffer=ByteBuffer.allocateDirect(batchSize);
                        long writePosition=0;
                          MappedByteBuffer mmap=fileChannel.map(FileChannel.MapMode.READ_WRITE,writePosition,batchSize*1024);
                        writePosition+=batchSize*1024;
                        for(long i=0;i<totalLength;i+=batchSize-2*1024){
                            byteBuffer.position(0).limit(batchSize);
                              mmap.put(byteBuffer);
                              mmap.force();
                              if(mmap.remaining()==0){
                                  MappedByteBuffer tmmap=mmap;
                                  pool.execute(()->{
                                      ((DirectBuffer)tmmap).cleaner().clean();
                                  });
                                  mmap=fileChannel.map(FileChannel.MapMode.READ_WRITE,writePosition,batchSize*1024);
                                  writePosition+=batchSize*1024;
                              }
                            fileChannel.write(byteBuffer);
                            fileChannel.force(false);
                        }
                        System.out.println("write cost1:"+(System.currentTimeMillis()-start));
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            };
        }
        for(Thread thread:threads){
            thread.start();
        }
        try {
            for(Thread thread:threads){
                thread.join();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
