package io.openmessaging.common;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chender
 * @date 2021/9/7 9:18
 */
public class Define {

    //是否开启“部分缓存”功能，以使得查询时，没必要为了一小点数据就去加载一个4k块
    //该功能对有利于缓解磁盘的吞吐压力，但是并不能缓解磁盘的iops压力，可根据实际情况进行配置
    public static boolean partCacheEnable=false;
    public static int partCacheLimit=1024;

    //是否严格遵守集合数量，如果严格准守，上一批次扣留了n个，下一批次就只能来joinCount-n个
    //严格模式下，整体写入会比较平稳，非严格模式下，可能会因为偶然的数据大小不均匀，导致短期的刷盘速度的波动(当然，也有可能往好的方向波动)
    public static boolean stickJoinCount=true;


    public static long startTime=System.currentTimeMillis();

    public static String ESSDPATH;
    public static String PMEMPATH;

    public static int maxForceJoinCount=8;//最大聚合请求数

    public static final int psThreads=4;

    public static final int metaLengthPerRequest=15;//  1 + 2 + 2 + 2 + 8   topicByte+offsetShort+queueIdShort+lengthShort+positonLong

    public static final boolean synWriteCache=false;//是否同步写cache


    public static final long firstStateTotalLength=75L*1024*1024*1024;//一阶段总数据量

    public static final long allDataLength=130L*1024*1024*1024;//总数据量

    public static final byte fourKFillTag=-1;//4k填充标识
    public static final byte emptyDataTag=-2;//空白数据标识

    public static final int maxTopicCount=100; //最大topic数量
    public static final int maxQueueCount=5000; //最大queue数量
    public static final int maxFetchCount=100; //一次最多查询的量
    public static final int minDataLength=100; //最小数据库的大小
    public static final int maxDataLength=17*1024; //最大数据量的大小

    public static int joinWaitTime=5;

    public static final int shareCacheMaxState=maxDataLength/1024;//共享缓存逻辑中，按缓存余量进行分级时的最大级别数

    public static final int memoryAllocateUnit =32*1024*1024; //每次申请内存的大小

    public static final int cacheCapacityPerBlock =32*1024;   //缓存块粒度
    public static final long aepCapacity=63L*1024*1024*1024;  //aep最大容量
    public static final int aepBlockCount =(int)(aepCapacity/ cacheCapacityPerBlock);

    public static final long directCacheMemory=2L*1024*1024*1024-256*1024*1024; //堆外缓存大小

    //堆内缓存大小，缓存肯定会被放到老年代，老年代越大，新生代就会越大，新生代太大了是浪费，所以堆内缓存就意思一下吧
    //让jvm所占的内存尽量少（这样新生代的浪费也小），然后使用mmap的copy on write 模式来作为缓存
    public static final long heapCacheMemory=1L*32*1024*1024;
    public static final long mmapCacheMemory=1L*512*1024*1024;  //单个mmap缓存大小
    public static final long mmapCacheMemoryCount=11;//mmap 数量
    public static final int memoryBlockCount=(int)((directCacheMemory+heapCacheMemory+mmapCacheMemory*mmapCacheMemoryCount)/ cacheCapacityPerBlock);


    public static final int maxThreads=50; //最大线程数
    public static final int maxAllDataLength=maxThreads*maxDataLength;  //一批数据的最大大小，17k*50

    public static int memorySkipSizeLimit=Integer.MAX_VALUE; //需要跳过缓存存储的数据块大小阈值，cacheService中有策略进行计算,并设置值

    public  static final AtomicLong freeCacheMemory=new AtomicLong();


    public static int forceJoinCount=10;//初始化请求聚合数量，该初始值意义不大，因为每隔几十毫秒，线程控制器就会根据实际的并发更新该值
    public static boolean doQuery=false; //是否已经开始插叙
    public static final int pageSize=4*1024; //4k

    static{
        if(new File("e:/tianchi/mq").exists()){//本地模式
            ESSDPATH="e:/tianchi/mq/essd";
            PMEMPATH="e:/tianchi/mq/pmem";
        }else{
            ESSDPATH="/essd";
            PMEMPATH="/pmem";
        }
    }

    public static void printTime(String scene){
        System.out.println(scene+"@"+(System.currentTimeMillis()-startTime));
    }
}
