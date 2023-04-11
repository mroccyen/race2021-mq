package io.openmessaging.control;

import io.openmessaging.common.Define;

/**
 * 通过统计近期的活跃线程数，动态计算聚合数目
 * 当线程活跃时会调用activeWrite方法，然后会通过一个数组把各个线程的最新活跃时间记载下来，线程自增序号就是数组的下标
 * 由于每次activeWrite的时候都去调用系统时间，会存在性能浪费
 * 所以有一个轮询线程，每一段时间会刷新一下当前时间，然后activeWrite的时候就直接用这个当前时间
 * //只要保证最近活跃逻辑的统计时间范围是上述刷新时间的两倍，就不会出现统计逻辑的问题
 * @author chender
 * @date 2021/9/29 22:19
 */
public class ThreadController {

    private static final int curTimeRefreshGap=10;//当前时间刷新间隔

    private static int updateTimeSpan=1000;

    private long sumCount;
    private int paramCount;

    private long[] lastActiveTimes=new long[Define.maxThreads];
    private long now;


    public void start(){
        new Thread(){
            @Override
            public void run() {
                while(true){
                    now=System.currentTimeMillis();
                    try {
                        Thread.sleep(curTimeRefreshGap);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        new Thread(){
            @Override
            public void run() {
                while(true){
                    try {
                        Thread.sleep(500);
                    }catch (Exception e){//先让程序跑起来
                        e.printStackTrace();
                    }
                    try {
                        Thread.sleep(curTimeRefreshGap*2);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    updateThreadsJoinParam();
                }
            }
        }.start();
    }


    /**
     * 线程活跃埋点
     * @param threadIdx 线程自增序号
     */
    public void activeWrite(int threadIdx){
        lastActiveTimes[threadIdx]=now;
    }

    /**
     * 更新线程聚合参数
     */
    public void updateThreadsJoinParam(){
        int count=activeCount(updateTimeSpan);
        //非严格聚合数量模式下，要-1，是因为核心逻辑里面，为了刷盘4k对齐，有一个扣留上一批次请求的逻辑
        //严格聚合数量模式下自己会考虑扣留的实际数量，进行动态的增量聚合
        Define.forceJoinCount=count/(Define.psThreads)-(Define.stickJoinCount?0:1);
        if(Define.forceJoinCount<=0){//活跃线程数小于等于刷盘线程数，不聚合
            Define.forceJoinCount=1;
        }
        paramCount++;
        sumCount+=Define.forceJoinCount;
        double rate=1D*Define.forceJoinCount/(1D*sumCount/paramCount);
        if(rate<0.8){//线程明显减少,需要更加密集地进行更新,join的等待时间也需要减短
            updateTimeSpan=10;
            Define.joinWaitTime=1;
        }else{
            updateTimeSpan=500;
            Define.joinWaitTime=5;
        }
    }


    /**
     * 统计时间窗口内的活跃线程数
     * @param timespan
     * @return
     */
    private int activeCount(int timespan){
        long curTime=now;
        int count=0;
        for(long at:lastActiveTimes){
            if(curTime-at<timespan){
                count++;
            }
        }
        return count;
    }
}
