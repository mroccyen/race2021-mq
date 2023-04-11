package io.openmessaging.ps;

import io.openmessaging.common.Define;
import io.openmessaging.common.ResourceManager;
import io.openmessaging.common.WriteRequest;
import io.openmessaging.ps.cache.CacheRequest;
import io.openmessaging.ps.cache.CacheService;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

import static java.nio.file.StandardOpenOption.*;

/**
 * 数据存储与查询的核心服务
 * @author chender
 * @date 2021/9/11 18:48
 */
public class MQService extends Thread {


    //异步聚合模式下，接受请求的queue
    private ArrayBlockingQueue<WriteRequest> requestReceiveQueue= new ArrayBlockingQueue<>(Define.maxThreads);

    //聚合后的待写入数据的queue,如果是严格聚合数量模式，每个刷盘线程一个queue，反之，共用一个queue(数组里面是同一个引用)
    private ArrayBlockingQueue<List<WriteRequest>>[] writeQueues =new ArrayBlockingQueue[Define.psThreads];

    private ArrayBlockingQueue<StrickJoinParam> joinParamQueue;
    private StrickJoinParam[] strickJoinParams;

    //数据块的元数据，以topic+queueId为维度
    private DataMeta[][] metaCaches;

    //缓存服务，包装了aep缓存、堆内堆外缓存
    private CacheService cacheService;

    //当一次查询的磁盘数据过多时，会多线程查询，所以需要一个线程池，极端情况是所有线程都在查询，所以大小为Define.maxThreads
    private Executor diskQueryThreadPool = Executors.newFixedThreadPool(Define.maxThreads);

    //查询的时候的基于线程线程的可复用资源
    private ThreadLocal<ConsumeContext> consumeContextThreadLocal = ThreadLocal.withInitial(() -> new ConsumeContext());

    private Unsafe unsafe = ResourceManager.getUnsafe();

    //刷盘chanel
    private FileChannel[] writeChanels=new FileChannel[Define.psThreads];

    //刷盘内存buffer，用于将一批请求中的数据复制到一块连续的内存中
    private ByteBuffer[] writeByteBuffers=new ByteBuffer[Define.psThreads];

    //writeByteBuffers对应的内存地址，为了用unsafe进行操作
    private long[] writeAddress =new long[Define.psThreads];

    //ssd上文件的当前写入位置
    private long[] writePosition=new long[Define.psThreads];

    //为了写入的时候4k对齐，但是又不想通过填充数据的方式来解决(填充数据会导致整体落盘的数据变大,大概3个多G，可以通过公式推算的)
    //所以对于每一批数据最后没有4k对齐的那一部分,会和下次的数据一起刷盘，所以也需要把这部分没对齐的数据对于的请求也缓存起来，下一次进行notify（严格做到force落盘后调用才返回）
    private List<WriteRequest>[] suffixRequest=new List[Define.psThreads];

    /**
     * 构造方法，初始化各种资源
     * @throws Exception
     */
    public MQService() throws Exception {
        cacheService = new CacheService();//构建缓存服务
        cacheService.start();//开启缓存任务(异步写缓存)

        this.metaCaches = new DataMeta[Define.maxTopicCount][Define.maxQueueCount];

        //按照赛题要求的100*5000进行Meta资源的初始化
        for (int i = 0; i < Define.maxTopicCount; i++) {
            for (int j = 0; j < Define.maxQueueCount; j++) {
                this.metaCaches[i][j] = new DataMeta();
            }
        }


        //严格聚合模式下的资源初始化， 各个刷盘线程的消费队列进行隔离
        if(Define.stickJoinCount){
            joinParamQueue=new ArrayBlockingQueue<>(Define.psThreads);
            strickJoinParams=new StrickJoinParam[Define.psThreads];
            for(int i=0;i<Define.psThreads;i++){
                joinParamQueue.offer(strickJoinParams[i]=new StrickJoinParam(i,0));
                writeQueues[i]=new ArrayBlockingQueue<>(1);//因为只有在上一个任务处理后，才能会放入下一个任务，所以容量设置为1
            }

        }else{
            ArrayBlockingQueue<List<WriteRequest>> queue=new ArrayBlockingQueue<>(Define.maxThreads);//非严格聚合数量模式下，最坏情况是不聚合(1次一条)，所以容量设置成Define.maxThreads
            for(int i=0;i<Define.psThreads;i++){
                writeQueues[i]=queue;//多个刷盘线程共用一个queue
            }
        }

        boolean doRecover=new File(Define.ESSDPATH + File.separator + "0.data").exists();
        if (doRecover) {//文件已经存在了，恢复数据
            recover();
        }
        ByteBuffer byteBuffer=ByteBuffer.allocateDirect(1024*1024);
        //为了测评阶段写入更快，所以在构造方法里面对文件进行了初始化写入(和pagecache初始化有关)
        //写入的数据大小会略微超过最终落盘的数据大小，超出的这部分在数据恢复的时候会造成干扰
        //所以初始化时，文件内容全部写成-2，在恢复阶段，如果取出来的topicId为-2，那说明就结束了(因为正常的topicId不可能为-2)
        //为什么不用-1？ 因为-1也是一个内置的值，在极端情况下，为了追求4k，程序会对数据进行填充，所以需要写入-1，标识开始填充了，恢复的时候就直接跳到下一个4k对齐的地方进行恢复
        unsafe.setMemory(((DirectBuffer)byteBuffer).address(),1024*1024,Define.emptyDataTag);
        for(int i=0;i<Define.psThreads;i++){
            suffixRequest[i]=new ArrayList();
            File file = new File(Define.ESSDPATH + File.separator + i + ".data");
            file.createNewFile();
            writeChanels[i]= FileChannel.open(file.toPath(),  WRITE);
            writeByteBuffers[i] = ByteBuffer.allocateDirect(Define.maxAllDataLength+Define.pageSize);//最坏情况是50个线程，每个线程都是17kb，然后上批次还有一个4k-1b的尾巴
            writeAddress[i]=((DirectBuffer) writeByteBuffers[i]).address();
            if(!doRecover){//正确性测试阶段不需要初始化文件，毕竟对速度没有要求
                //这个操作会直接让写入速度从290m/s提升到320m/s，一方面是和pageCache元数据的初始化有关，另外一方面和ssd的内部机制有关
                //这像极了用unsafe申请了一块内存，如果不初始化(unsafe.setMemory)，后续首次访问就会很慢，如果初始化了，后续访问就非常快
                //在之前参加adb的的比赛的时候，就对这个问题非常的疑惑，一直没找到想要的答案
                //目前猜测大概率是因为在申请内存后，只是返回了一个内存的起始地址，但是应用程序访问的都是虚拟内存，所以虚拟内存和物理内存之间的page映射还没建立，该映射会在首次访问的时候进行
                //同理，ssd文件的初始化也是这么个道理，所以先写一遍，后面就会非常快
                for(long j=0;j<Define.allDataLength/Define.psThreads;j+=1024*1024){
                    byteBuffer.clear();
                    writeChanels[i].write(byteBuffer);
                    writeChanels[i].force(false);
                }
                writeChanels[i].position(0);
            }
        }
        ((DirectBuffer)byteBuffer).cleaner().clean();//释放堆外内存
        startWriteThread();//开启刷盘线程
    }

    /**
     * 进行数据恢复
     * @throws Exception
     */
    private void recover() throws Exception {
        ByteBuffer metaByteBuffer = ByteBuffer.allocateDirect(Define.metaLengthPerRequest);
        long address=((DirectBuffer)metaByteBuffer).address();
        for (int i = 0; i < Define.psThreads; i++) {
            File file = new File(Define.ESSDPATH + File.separator + i + ".data");
            FileChannel channel = FileChannel.open(file.toPath(), READ);
            long dataLength = file.length();
            long filePosition = 0;
            while (true) {
                metaByteBuffer.clear();
                channel.position(filePosition);
                channel.read(metaByteBuffer);
                long readAddress=address;
                byte topicByte = unsafe.getByte(readAddress++);
                if (topicByte == Define.fourKFillTag) {//该位置上进行了4k对齐的填充,跳到下一个4k对齐的地方
                    filePosition = (filePosition / Define.pageSize + 1) * Define.pageSize;
                }else if(topicByte==Define.emptyDataTag){//读到空白的无人区了，结束了
                    break;
                } else {
                    int dataOffset = unsafe.getShort(readAddress);
                    readAddress+=2;
                    short queueId = unsafe.getShort(readAddress);
                    readAddress+=2;
                    short length = unsafe.getShort(readAddress);
                    readAddress+=2;
                    long position = unsafe.getLong(readAddress);
                    metaCaches[topicByte][queueId].push((byte)i, position, length, dataOffset);
                    filePosition += Define.metaLengthPerRequest + length;//跳到下一个meta数据的位置
                }

                if (filePosition >= dataLength) {//读到文件末尾了，结束
                    break;
                }
            }
        }
        //虽然一个queueId的数据的offset在单个文件中是有序的，但是多个文件顺序接续完之后，offset就无需了（会出现：1、3、5、2、4、6这样的dataset序列）
        //所以还需要对元数据继续遍历，将meta放到自己的offset对应的位置
        for (int i = 0; i < metaCaches.length; i++) {
            DataMeta[] dataMetas = metaCaches[i];
            for (DataMeta dataMeta : dataMetas) {
                List<MetaItem> metas = new ArrayList<>(dataMeta.metas);
                for (int j = 0; j < metas.size(); j++) {
                    MetaItem tmp = metas.get(j);
                    //tmp.cachePosition存的其实是dataOffset，为了节约内存，所以在上面使用了cachePosition来临时存储dataOffset的值
                    dataMeta.metas.set((int)tmp.cachePosition, new MetaItem(tmp.fileIdx, tmp.filePosition, tmp.dataLength,-1));
                }
            }
        }
    }

    /**
     * 通过队列异步聚合多线程数据
     */
    @Override
    public void run() {
        try {
            while (true) {
                int detain=0;
                int psThreadIdx=0;
                if(Define.stickJoinCount){//严格聚合模式
                    StrickJoinParam sjp=joinParamQueue.take();
                    detain=sjp.detainNo;
                    psThreadIdx=sjp.psThreadIdx;

                }
                int joinNo=Define.forceJoinCount-detain;//需要聚合的数量=预期数量-上批次扣留的数量
                if(joinNo<1){//会存在小于1的情况，比如上批次扣留了2，让后当前由于线程数骤降，预期聚合数变成1了，随意就会得到-1
                    joinNo=1;
                }
                List<WriteRequest> writeRequestList = new ArrayList<>(joinNo);
                while (true) {
                    //由于有线程聚合数控制器的逻辑，所以这里就算poll 5ms也很ok，基本不会出现已经获取到几块数据了，然后持有着这几块数据傻傻等5ms还等不到数据的情况
                    WriteRequest writeRequest = requestReceiveQueue.poll(Define.joinWaitTime,TimeUnit.MILLISECONDS);
                    if (writeRequest == null) {
                        if (writeRequestList.size() == 0) {//上游没有数据过来，继续poll
                            continue;
                        } else {//已经持有一部分数据了，先处理了吧（基本只会在收尾阶段，或其他原因导致生产线程数量发生变化的时候，才会出现该情况，该情况会在很短的时间内被ThreadController修正）
                            break;
                        }
                    }
                    writeRequestList.add(writeRequest);
                    if (writeRequestList.size() >= joinNo) {//凑齐了
                        break;
                    }
                }
                writeQueues[psThreadIdx].offer(writeRequestList);//容量保证了不会offer失败
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    /**
     * 接收一个请求
     * @param writeRequest
     */
    public void receiveRequest(WriteRequest writeRequest){
        requestReceiveQueue.offer(writeRequest);//queue的容量是最大线程数，所以不可能offer失败
    }

    /**
     * 开启异步写盘线程
     */
    private void startWriteThread() {
        for (int i = 0; i < Define.psThreads; i++) {
            int fi = i;
            new Thread() {
                @Override
                public void run() {
                    try {
                        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                        doWrite(fi);
                    } catch (Exception e) {
                        Define.printTime("doWrite exception");
                        System.exit(-1);
                    }
                }
            }.start();
        }
    }

    /**
     * 循环写入聚合的数据(异步聚合模式下的逻辑)
     * @param idx
     */
    private void doWrite(int idx) {
        try {
            while (true) {
                List<WriteRequest> writeRequestList =writeQueues[idx].poll(5,TimeUnit.MILLISECONDS);
                if(writeRequestList==null){
                    //当拿不到更多数据的时候，把上一批的尾巴写了，理论上只有在最后块结束的时候、或者系统出现卡顿的时候才会触发该逻辑
                    //该逻辑可以保证写入调用不会一直被阻塞住
                    if(suffixRequest[idx].size()!=0){
                        int length=writeByteBuffers[idx].position();
                        int gap=Define.pageSize-length;//未满的尺寸
                        writeByteBuffers[idx].put((byte)(-1));//跳空标识
                        writeByteBuffers[idx].position(0).limit(Define.pageSize);
                        writeChanels[idx].write(writeByteBuffers[idx]);
                        writeChanels[idx].force(false);
                        writeByteBuffers[idx].clear();
                        writePosition[idx]+=gap;
                        for(WriteRequest writeRequest:suffixRequest[idx]){//唤醒请求线程
                            writeRequest.lock.lock();
                            writeRequest.condition.signal();
                            writeRequest.lock.unlock();;
                        }
                        suffixRequest[idx].clear();//清空扣留列表
                    }
                    writeRequestList=writeQueues[idx].take();
                }
                writeOnce(idx,writeRequestList);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * 动态排序，让后续可能从磁盘加载的块，尽可能地有一边4k对齐(未完成，这个问题太复杂了，是一个NP问题)
     * @param list
     * @param curPosition
     */
    private void smartSort(List<WriteRequest> list,int curPosition){
     List<WriteRequest> big=new ArrayList<>();
     List<WriteRequest> small=new ArrayList<>();
      for(WriteRequest request:list){
          int length=request.getData().remaining();
          if(!Define.doQuery&&length>Define.memorySkipSizeLimit){//后续可能查ssd的请求
              big.add(request);
          }else{
              small.add(request);
          }
      }
//      if(big.size()==0)
    }



    public void writeOnce(int idx,List<WriteRequest> writeRequestList)throws Exception{
//        //找到最大的块，放在前面，因为大块大概率是不会写缓存的，所以放第一位，首部4k对齐的，后续从ssd读取时效益更高（该逻辑只会在完全填充模式下有效，但是完全填充模式不是最优了，所以注释掉了）
//        if(writeRequestList.size()>1){
//            int maxIdx=-1;
//            int maxLength=0;
//            for(int i=0;i<writeRequestList.size();i++){
//                if(writeRequestList.get(i).getData().limit()>maxLength){
//                    maxIdx=i;
//                    maxLength=writeRequestList.get(i).getData().limit();
//                }
//            }
//            WriteRequest first=writeRequestList.get(0);
//            writeRequestList.set(0,writeRequestList.get(maxIdx));
//            writeRequestList.set(maxIdx,first);
//        }

//        if(!Define.doQuery&&writeRequestList.size()>2){
//            smartSort(writeRequestList,writeByteBuffers[idx].position());
//        }

        writeRequestList.sort(Comparator.comparingLong((a)->a.requestCount));//按历史请求数重新到大进行排序，这样请求大的线程在末尾，容易被扣留，使得所有线程的请求速度相对均匀;

        long unsafeAddress = writeAddress[idx]+writeByteBuffers[idx].position();
        int suffixSum=writeByteBuffers[idx].position();

        CacheRequest cacheRequest =null;
        for (WriteRequest writeRequest : writeRequestList) {
            byte topicByte=Byte.parseByte(writeRequest.getTopic().substring(5));
            DataMeta dataMeta = metaCaches[topicByte][writeRequest.getQueueId()];
            int dataLength = writeRequest.getData().remaining();
            writePosition[idx] += Define.metaLengthPerRequest;
            MetaItem metaItem = dataMeta.pushWithAep((byte)idx, writePosition[idx], (short) dataLength, -1);//存入meta数据


            //写入阶段，缓存是不够的，所以要基于规划的Define.memorySkipSizeLimit值，将大块过滤出来，不放缓存(因为有限的缓存中放入小块会更nice)
            //如果大块两端的4k对齐情况非常不好（都是略微越界一点点的那种），会将越界的数据放缓存，这样查询的时候就能按4k对齐进行加载了
            if (!Define.doQuery&&dataLength>Define.memorySkipSizeLimit) {
                int preLength=Define.pageSize-(int)(writePosition[idx]%Define.pageSize);
                int suffixLength=(int)((writePosition[idx]+dataLength)%Define.pageSize);

                if(Define.partCacheEnable&&(preLength<Define.partCacheLimit||suffixLength<Define.partCacheLimit)){//某个4k里面实际数据太少了，将这少量的数据放缓存吧，避免从ssd加载效益不高的问题
                    if(cacheRequest==null){
                        cacheRequest= cacheService.getWriteDatas();
                        cacheRequest.count = 0;
                    }
                    cacheRequest.topics[cacheRequest.count] = topicByte;
                    cacheRequest.queueIds[cacheRequest.count] = writeRequest.getQueueId();
                    cacheRequest.dataLengthes[cacheRequest.count] = 0;


                    int offset=0;
                    if(preLength<Define.partCacheLimit){//前面一小节放缓存
                        writeRequest.getData().get(cacheRequest.datas[cacheRequest.count], 0, preLength);
                        cacheRequest.dataLengthes[cacheRequest.count]+=preLength;
                        offset+=preLength;
                    }
                    if(suffixLength<Define.partCacheLimit){//后面一小节放缓存
                        writeRequest.getData().position(dataLength-suffixLength);
                        writeRequest.getData().get(cacheRequest.datas[cacheRequest.count], offset, suffixLength);
                        cacheRequest.dataLengthes[cacheRequest.count]+=suffixLength;
                    }
                    cacheRequest.metas[cacheRequest.count] = metaItem;
                    cacheRequest.partCache[cacheRequest.count]=true;
                    cacheRequest.count++;
                    writeRequest.getData().position(0).limit(dataLength);
                }
            } else{
                if(cacheRequest==null){
                    cacheRequest= cacheService.getWriteDatas();
                    cacheRequest.count = 0;
                }
                cacheRequest.topics[cacheRequest.count] = topicByte;
                cacheRequest.queueIds[cacheRequest.count] = writeRequest.getQueueId();
                writeRequest.getData().get(cacheRequest.datas[cacheRequest.count], 0, dataLength);
                cacheRequest.dataLengthes[cacheRequest.count] = dataLength;
                cacheRequest.metas[cacheRequest.count] = metaItem;
                cacheRequest.partCache[cacheRequest.count]=false;
                writeRequest.getData().position(0).limit(dataLength);
                cacheRequest.count++;
            }

            writeRequest.setOffset(dataMeta.metas.size()-1);

            unsafe.putByte(unsafeAddress++,topicByte);
            unsafe.putShort(unsafeAddress,(short)(dataMeta.metas.size()-1));
            unsafeAddress+=2;
            unsafe.putShort(unsafeAddress,(short) writeRequest.getQueueId());
            unsafeAddress+=2;
            unsafe.putShort(unsafeAddress,(short) dataLength);
            unsafeAddress+=2;
            unsafe.putLong(unsafeAddress,writePosition[idx]);
            unsafeAddress+=8;
            writeByteBuffers[idx].position((int)(unsafeAddress-writeAddress[idx]));
            writeByteBuffers[idx].put(writeRequest.getData());
            unsafeAddress+=dataLength;

            writePosition[idx] += dataLength;
        }
        if(cacheRequest!=null){
            if(Define.synWriteCache){
                cacheService.synWriteDatas(cacheRequest);
            }else{//
                cacheService.asynWriteDatas(cacheRequest);
            }
        }


        int length = writeByteBuffers[idx].position();//待写入的长度
        int left = length % Define.pageSize;//溢出长度
        int writeLength=length;

        if(left!=0){
            if(length>Define.pageSize){//未4k对齐的尾巴本次不写
                writeLength-=left;
                writeByteBuffers[idx].position(0).limit(writeLength);
            }else{//没满4k，填充
                int addPosition = Define.pageSize - writeByteBuffers[idx].position() % Define.pageSize;
                writePosition[idx] += addPosition;
                writeByteBuffers[idx].put(Define.fourKFillTag);//4k填充标识
                writeByteBuffers[idx].limit(writeByteBuffers[idx].position() + addPosition - 1).position(0);
            }
        }else{//天然对齐，完美 1/4096的概率
            writeByteBuffers[idx].position(0).limit(length);
        }

        writeChanels[idx].write(writeByteBuffers[idx]);
        writeChanels[idx].force(false);

        if(suffixRequest[idx].size()!=0){//上一批次留下的尾巴(小于4k)一定已经落盘了，因为若本批次数据大于4k，那第一个4kb肯定落盘了，若小于4k,也进行了填充落盘，所以不存在上一批次的尾巴在本批次仍然是尾巴的情况
            for(WriteRequest writeRequest:suffixRequest[idx]){
                writeRequest.lock.lock();
                writeRequest.condition.signal();
                writeRequest.lock.unlock();
            }
            suffixRequest[idx].clear();
        }

        int backSuffixSum=suffixSum;
        for(WriteRequest writeRequest:writeRequestList){//记录本批次扣留的请求
            suffixSum+=writeRequest.getData().position()+Define.metaLengthPerRequest;
            if(suffixSum>writeLength){//还有部分数据没落盘，不能唤醒
                suffixRequest[idx].add(writeRequest);
            }
        }
        strickJoinParams[idx].detainNo=suffixRequest[idx].size();
        joinParamQueue.offer(strickJoinParams[idx]);

        suffixSum=backSuffixSum;
        for(WriteRequest writeRequest:writeRequestList){//唤醒请求线程
            suffixSum+=writeRequest.getData().position()+Define.metaLengthPerRequest;
            if(suffixSum<=writeLength){
                writeRequest.lock.lock();
                writeRequest.condition.signal();
                writeRequest.lock.unlock();
            }
        }

        if(suffixRequest[idx].size()!=0){//存在扣留数据
            //此时position在4k处，将limit设在到实际数据的位置，将尾巴compact到头部
            writeByteBuffers[idx].limit(length);
            writeByteBuffers[idx].compact();
        }else{
            writeByteBuffers[idx].clear();
        }
    }

    /**
     * 消费数据
     * @param topic
     * @param queueId
     * @param offset
     * @param fetchNum
     * @return
     * @throws Exception
     */
    public Map<Integer, ByteBuffer> consumeData(String topic, int queueId, long offset, int fetchNum) throws Exception {
        ConsumeContext context = consumeContextThreadLocal.get();
        context.results.clear();
        byte topicByte =Byte.parseByte(topic.substring(5));
        DataMeta dataMeta = metaCaches[topicByte][queueId];
        ByteBuffer[] byteBuffers = context.byteBuffers;
//        byte[]bytes = context.bytes;
        byte[][] bytes = context.bytes;
        int idx = (int) offset;

        List<Integer> diskIdxes=new ArrayList<>(fetchNum);
        List<Integer> cacheIdxes=new ArrayList<>(fetchNum);


        boolean batch=true;

        //分离出哪些是需查ssd的，哪些是需要查缓存的
        long maxCachePosition=0;

        for (int i = 0; i < fetchNum; i++) {
            MetaItem meta = dataMeta.get(idx++);
            if (meta == null) {
                break;
            }

            if(meta.cachePosition!=-1&&meta.cachePosition<Integer.MAX_VALUE){//有缓存，且是非共享缓存
                int cachePosition=(int)meta.cachePosition;
                if(cachePosition<-1){
                    cachePosition+=Integer.MAX_VALUE;
                }
                if(cachePosition>maxCachePosition){//不是共享缓存且未缓存或者部分缓存(未缓存或者部分缓存时cachePosition都为负数，不可能大于maxCachePosition)
                    maxCachePosition=cachePosition;//记录下最大的缓存position，然后将该position之前的缓存都释放掉；
                }
            }

            if((int)meta.cachePosition>=0){//不为-1(-1代表未缓存)，或者低位不为负数(为负数代表的是“部分缓存”)，数据从缓存中获取
                batch=batch&&meta.cachePosition<Integer.MAX_VALUE;//没有使用共享内存才能批量加载
                cacheIdxes.add(idx-1);
            }else{//从磁盘获取
                diskIdxes.add(idx-1);
            }
        }

        int cacheSize=cacheIdxes.size();
        if(cacheSize!=0){//存在查缓存的情况，看下这些个缓存是否满足连续copy的条件
            if(cacheSize>1&&batch){
                for(int i=1;i<cacheSize;i++){
                    if(cacheIdxes.get(i)-cacheIdxes.get(i-1)!=1){//如果需要访问的两个块在缓存块上不是连续的，则不能批量加载
                        batch=false;
                        break;
                    }
                }
            }else{
                batch=false;
            }
            batch=false;
            //批量加载的逻辑有间歇性bug，看不了日志了，屏蔽吧，不想程序不健壮
            if(batch){
//                Define.batchAepCount.incrementAndGet();
//                aepService.batchReadData(topicByte, queueId, dataMeta,cacheIdxes,bytes);
//                int totalLength=0;
//                for(int i=0;i<cacheSize;i++){
//                    long[] meta=dataMeta.get(cacheIdxes.get(i));
//                    byteBuffers[i].limit(totalLength+(int)meta[2]).position(totalLength);
//                    totalLength+=(int)meta[2];
//                    context.results.put(i, byteBuffers[i]);
//                }
            }else{
                for(int i=0;i<cacheSize;i++){
                    MetaItem meta=dataMeta.get(cacheIdxes.get(i));
//                    aepService.readData(topicByte, queueId, meta[3],bytes,i*Define.maxDataLength,(int) meta[2]);
//                    byteBuffers[i].limit(i*Define.maxDataLength+(int) meta[2]).position(i*Define.maxDataLength);
                    cacheService.readData(topicByte, queueId, meta.cachePosition,bytes[i],0,meta.dataLength);
                    byteBuffers[i].position(0).limit((int) meta.dataLength);
                    context.results.put(cacheIdxes.get(i)-(int)offset, byteBuffers[i]);
                }
            }

        }
        if(diskIdxes.size()!=0){//存在查ssd的情况
            int split=diskIdxes.size()/2;
            if(diskIdxes.size()%2==1){//尝试将数据分成两份
                split++;
            }
            if(diskIdxes.size()<10){//数量太少，不多线程加载
                split=diskIdxes.size();
            }
            CompletableFuture<Void> future=null;
            if(diskIdxes.size()>=10){//不止一块数据，可以进行双线程读取
                final int fSplit=split;
                future=CompletableFuture.runAsync(()->{
                    try {
                        for(int i=fSplit;i<diskIdxes.size();i++){
                            MetaItem meta=dataMeta.get(diskIdxes.get(i));
                            long rPosition=meta.filePosition;
                            byteBuffers[i+cacheSize].position(0).limit(meta.dataLength);
                            if(meta.cachePosition<-1){//部分缓存
                                int preLength=Define.pageSize-(int)(meta.filePosition%Define.pageSize);//首部4k溢出部分的长度
                                int suffixLength=(int)((meta.filePosition+meta.dataLength)%Define.pageSize);//尾部4k溢出部分的长度
                                int cacheLength=0;
                                if(preLength<Define.partCacheLimit){
                                    byteBuffers[i+cacheSize].position(preLength);
                                    cacheLength+=preLength;
                                    rPosition+=preLength;
                                }
                                if(suffixLength<Define.partCacheLimit){
                                    cacheLength+=suffixLength;
                                }
                                cacheService.readData(topicByte, queueId, meta.cachePosition,bytes[i+cacheSize],0,cacheLength);
                                if(suffixLength<Define.partCacheLimit){
                                    System.arraycopy(bytes[i+cacheSize],byteBuffers[i+cacheSize].position(),bytes[i+cacheSize],meta.dataLength-suffixLength,suffixLength);
                                    byteBuffers[i+cacheSize].limit(meta.dataLength-suffixLength);
                                }
                            }
                            context.channels[(int) meta.fileIdx+Define.psThreads].position(rPosition).read(byteBuffers[i+cacheSize]);
                            byteBuffers[i+cacheSize].position(0).limit(meta.dataLength);
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                        System.exit(-1);
                    }
                },diskQueryThreadPool);
            }
            for(int i=0;i<split;i++){
                MetaItem meta=dataMeta.get(diskIdxes.get(i));
                long rPosition=meta.filePosition;
                byteBuffers[i+cacheSize].position(0).limit(meta.dataLength);
                if(meta.cachePosition<-1){//部分缓存
                    int preLength=Define.pageSize-(int)(meta.filePosition%Define.pageSize);
                    int suffixLength=(int)((meta.filePosition+meta.dataLength)%Define.pageSize);
                    int cacheLength=0;
                    if(preLength<Define.partCacheLimit){
                        cacheLength+=preLength;
                        rPosition+=preLength;
                        byteBuffers[i+cacheSize].position(preLength);
                    }
                    if(suffixLength<Define.partCacheLimit){
                        cacheLength+=suffixLength;
                    }
                    cacheService.readData(topicByte, queueId, meta.cachePosition,bytes[i+cacheSize],0,cacheLength);
                    if(suffixLength<Define.partCacheLimit){
                        System.arraycopy(bytes[i+cacheSize],byteBuffers[i+cacheSize].position(),bytes[i+cacheSize],meta.dataLength-suffixLength,suffixLength);
                        byteBuffers[i+cacheSize].limit(meta.dataLength-suffixLength);
                    }
                }
                context.channels[(int) meta.fileIdx].position(rPosition).read(byteBuffers[i+cacheSize]);
                byteBuffers[i+cacheSize].position(0).limit(meta.dataLength);
            }
            if(future!=null){
                future.get();
            }

            for(int i=0;i<diskIdxes.size();i++){
                context.results.put(diskIdxes.get(i)-(int)offset, byteBuffers[cacheSize+i]);
            }
        }

        dataMeta.clean((int)(offset+fetchNum));

        if(Define.partCacheEnable&&maxCachePosition!=0){//释放读取过的内存块
            cacheService.freeMemoryBlocks(topicByte,queueId,maxCachePosition);
        }

        return context.results;
    }



    public static class DataMeta {

        //每块数据的meta是一个长度为4的数组，存的是文件下标、文件位置、数据长度、缓存位置
        public List<MetaItem> metas = new ArrayList<>();

        public DataMeta() {

        }

        public void push(byte fileIdx, long position, short length, int dataOffset) {
            metas.add(new MetaItem(fileIdx, position, length, dataOffset));
        }

        public MetaItem pushWithAep(byte fileIdx, long position, short length, int aepPosition) {
            MetaItem item=new MetaItem(fileIdx, position, length, aepPosition);
            metas.add(item);
            return item;
        }

        public MetaItem get(int offset) {
            if (offset >= metas.size()) {
                return null;
            }
            return metas.get(offset);
        }

        /**
         * 消费之后就释放
         * @param offset
         */
        public void clean(int offset){
            if(offset>metas.size()){
                offset=metas.size();
            }
            while(--offset>=0){
                if(metas.get(offset)==null){
                    return;
                }else{
                    metas.set(offset,null);
                }
            }
        }
    }

    public static class MetaItem{
        public byte fileIdx;
        public long filePosition;
        public short dataLength;
        public long cachePosition;

        public MetaItem(byte fileIdx,long filePosition,short dataLengtth,long cachePosition){
            this.fileIdx=fileIdx;
            this.filePosition=filePosition;
            this.dataLength =dataLengtth;
            this.cachePosition=cachePosition;
        }
    }


    public static class StrickJoinParam{
        private int psThreadIdx;//刷盘线程编号
        private int detainNo;//上批次扣留数量

        public StrickJoinParam(int psThreadIdx,int detainNo){
            this.psThreadIdx=psThreadIdx;
            this.detainNo=detainNo;
        }

    }
}
