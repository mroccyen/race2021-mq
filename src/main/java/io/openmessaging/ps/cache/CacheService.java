package io.openmessaging.ps.cache;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryAccessor;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.common.Define;
import io.openmessaging.ps.MQService;
import io.openmessaging.ps.cache.block.*;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * @author chender
 * @date 2021/9/30 23:57
 */
public class CacheService extends Thread {
    private Heap heap;
    //空闲的Aep缓存块
    private ArrayBlockingQueue<CacheBlock> freeAepBlocks = new ArrayBlockingQueue(Define.aepBlockCount);

    //空闲的堆外、堆内缓存块，因为堆内堆外内存比aep要快一些，所以做了一个资源池的分级，优先循环使用堆内堆外的内存
    private ArrayBlockingQueue<CacheBlock> freeMemoryBlocks = new ArrayBlockingQueue(Define.memoryBlockCount);

    //队列缓存上下文，以topic和queueId为维度的二维数组，按照赛题要求，总共需要初始化100*5000=50w个TopicCacheContext对象
    private TopicCacheContext[][] topicContexts = new TopicCacheContext[Define.maxTopicCount][Define.maxQueueCount];

    //空闲的缓存请求资源池
    private ConcurrentLinkedQueue<CacheRequest> freeWriteQueue = new ConcurrentLinkedQueue<>();

    //待写入的缓存请求
    private ConcurrentLinkedQueue<CacheRequest> wrokWriteQueue = new ConcurrentLinkedQueue<>();

    //堆外缓存对应的byteBuffer，引用起来，避免被内存被回收
    private DirectBuffer cacheByteBuffer;
    private List<MappedByteBuffer> cacheMmaps=new ArrayList<>();

    //当空闲的缓存块耗尽时，每个queueId的在用的最后一块缓存块大概率是没有被写满的，所以可以共享出来，给其他的queueId用
    //由于没有ConcurrenHashSet，所以就用map了
    //这是一个map数组，下标代表容量，比如下标为1的map，里面存储的时候1kb到2kb余量的内存，余量大于Define.maxDataLengt(17Kb)的，全部存在下标为17的map中
    private Map<TopicCacheContext,Integer>[] sharedContextMaps = new Map[Define.maxDataLength / 1024 + 1];

    //在缓存耗尽的时候，会进行一次计算，将所有queueId的最后一个未满(且余量大于1kb)的内存块，进行编号，放到sharedBlocks数组中
    //如果一个写入的数据使用了共享数据块，会将对应共享块的编号存到元数据中，查询的时候，直接通过编号，从sharedBlocks获取对应的内存块，进行读取即可
    private CacheBlock[] sharedBlocks = new CacheBlock[Define.maxTopicCount * Define.maxQueueCount + 1];//因为一些原因，下标得从1开始

    //同步刷盘时的锁
    private ReentrantLock writeLock=new ReentrantLock();

    //还有共享的内存
    private boolean hasSharedMemory=true;
    private boolean cacheFulled = false;

    //用于缓存从磁盘上加载出来的没有4k对齐的数据
    private Map<Long,Integer> diskDataMap=new ConcurrentHashMap<>();
    private TopicCacheContext diskDataCaches=new TopicCacheContext();


    public CacheService() {

        for (int i = 0; i < sharedContextMaps.length; i++) {
            sharedContextMaps[i] = new ConcurrentHashMap();//TODO 利用随机分布的特性，预估容量
        }

        try {
            allocateMemory();//缓存初始化
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        //计算相关参数
        computeParam();


        //初始化缓存context
        for (int i = 0; i < Define.maxTopicCount; i++) {
            for (int j = 0; j < Define.maxQueueCount; j++) {
                topicContexts[i][j] = new TopicCacheContext();
            }
        }

        for (int i = 0; i < 256; i++) {
            freeWriteQueue.offer(new CacheRequest());
        }
    }

    /**
     * 计算相关参数，比如缓存服务的准入数据库大小
     * 因为始终是有一部分数据无法放到缓存中，那么我们肯定希望这些放不到缓存中（必须从ssd中读取）的数据都是尽量大的块
     * 那么到底数据库大于多少时，就不放缓存，可以是的最后刚好缓存被放满呢（无剩余意味着利用率高，不溢出意味着后面不会有小块从ssd进行读取）
     * 由于写入数据的大小是均匀分布的，所以根据三角形面积的相关理论，这个值是可以被计算出来的
     */
    private void computeParam() {
        long totalMemoryLength = 1L * (freeAepBlocks.size() + freeMemoryBlocks.size()) * Define.cacheCapacityPerBlock;//总缓存大小
        long overflowLength = Define.firstStateTotalLength - totalMemoryLength;//缓存存不下的数据大小;

        //三角形面积问题，(1-x)*(1-x)=cachePct => 1-x=Math.sqrt(cachePct) => x=1-Math.sqrt(cachePct)
        double cachePct = 1 - 1d * (overflowLength) / Define.firstStateTotalLength;
        System.out.println("cachePct:" + cachePct);
        double skipBlockLengthPct = 1 - Math.sqrt(cachePct);

        System.out.println("skipBlockLengthPct:" + skipBlockLengthPct / 1024D / 1024 / 1024 + "G");

        //基于数据块的大小是随机分布的，计算出阈值为多少时，可以使得最大的那些块不写缓存
        //这样，跳过的都是尽量大的数据块，整体块的数量会更小，到时候查essd的时候，效益会更高;同时小块放缓存里面性价比会更高
        //理论上应该减去Define.minDataLength/2，但是宁可有冗余，也不能超标，超标会导致小块需从ssd读，负面影响更大；
        Define.memorySkipSizeLimit = (int) (Define.maxDataLength * (1 - skipBlockLengthPct)) - Define.minDataLength*3/4;
        System.out.println("overFlowLength:" + overflowLength / 1024D / 1024 / 1024 + "G");
        System.out.println("memorySkipSizeLimit:" + Define.memorySkipSizeLimit / 1024D + "KB");
    }

    /**
     * 缓存分为aep缓存、堆内缓存、堆外缓存
     * 按照缓存性能进行了分级处理，程序会优先使用性能高的缓存，这样高性能的缓存会更容易被使用到
     * @throws Exception
     */
    private void allocateMemory() throws Exception {
        Define.printTime("inited aep");
        long maxHeapSize = Define.aepCapacity;
        //最大限度申请aep内存，这种方式有些暴力，用太满会导致有时候测评的时候出现aep访问非常慢的情况，可能和aep的一些内部机制有关吧
        while (true) {
            try {
                heap = Heap.createHeap(Define.PMEMPATH + File.separator + "aep", maxHeapSize);
            } catch (Exception e) {
                System.out.println("allocate " + maxHeapSize / 1024D / 1024 / 1024 + "G heap fail");
                maxHeapSize -= 128 * 1024 * 1024;//在危险的边缘疯狂试探
                continue;
            }
            System.out.println("allocate  " + maxHeapSize / 1024D / 1024 / 1024 + "G heap success");
            break;
        }

        //最大限度申请MemoryBlock
        long sumLength = 0;
        List<MemoryBlock> list = new ArrayList<>();
        try {
            for (int i = 0; i < 1000000; i++) {//在危险的边缘疯狂试探+1
                list.add(heap.allocateMemoryBlock(Define.memoryAllocateUnit));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("init " + list.size() + " memory block, total length:" + (1D * list.size() * Define.memoryAllocateUnit / 1024 / 1024 / 1024) + "G");
        }

        //MemoryBlock的本质也是通过unsafe对内存地址进行读写访问，所以直接提取其内存地址，这样MemoryBlock对象就能被回收了，减小堆内内存压力
        Method dAddressMethod = MemoryAccessor.class.getDeclaredMethod("directAddress");
        Method metaSizeMethod = MemoryAccessor.class.getDeclaredMethod("metadataSize");
        dAddressMethod.setAccessible(true);
        metaSizeMethod.setAccessible(true);

        for (int i = 0; i < list.size(); i++) {
            MemoryBlock memoryBlock = list.get(i);
            long baseAddress = (long) dAddressMethod.invoke(memoryBlock) + (long) metaSizeMethod.invoke(memoryBlock);
            memoryBlock.setByte(0, (byte) 1);//init
            for (int j = 0; j < Define.memoryAllocateUnit / Define.cacheCapacityPerBlock; j++) {
                freeAepBlocks.offer(new AepCacheBlock(baseAddress + j * Define.cacheCapacityPerBlock));
                sumLength += Define.cacheCapacityPerBlock;
                Define.freeCacheMemory.addAndGet(Define.cacheCapacityPerBlock);
            }
        }
        System.out.println("use aep block :" + (1D * sumLength / 1024 / 1024 / 1024) + "GB");


        //申请堆外缓存，cacheByteBuffer对象必须被引用，避免gc导致堆外内存被释放
        cacheByteBuffer = (DirectBuffer) ByteBuffer.allocateDirect((int) Define.directCacheMemory);
        long address = cacheByteBuffer.address();
        for (int i = 0; i < Define.directCacheMemory / Define.cacheCapacityPerBlock; i++) {
            freeMemoryBlocks.offer(new DirectMCacheBlock(address + i * Define.cacheCapacityPerBlock));
            Define.freeCacheMemory.addAndGet(Define.cacheCapacityPerBlock);
        }

        //申请mmap缓存
        for(int j=0;j<Define.mmapCacheMemoryCount;j++){
            FileChannel fileChannel=FileChannel.open(new File(Define.ESSDPATH+File.separator+"mmap.data"+j).toPath(),WRITE,CREATE,READ);
            MappedByteBuffer cacheMmap = fileChannel.map(FileChannel.MapMode.PRIVATE,0,Define.mmapCacheMemory);
            address = ((DirectBuffer)cacheMmap).address();
            ByteBuffer byteBuffer=ByteBuffer.allocateDirect(Define.cacheCapacityPerBlock);
            for (int i = 0; i < Define.mmapCacheMemory / Define.cacheCapacityPerBlock; i++) {
                freeMemoryBlocks.offer(new MMAPCacheBlock(address + i * Define.cacheCapacityPerBlock));
                Define.freeCacheMemory.addAndGet(Define.cacheCapacityPerBlock);
                byteBuffer.clear();
                cacheMmap.put(byteBuffer);
            }
            //疯狂地访问对应的pageCache，让这些pageCache权重更高，更不容易被淘汰
            //这种操作其实应该是没啥意义的，copy on write模式下，已经和pageCache脱节了
            //作为强迫症，还是加上吧，现在也看不了日志了，万一有效呢
            for(int i=0;i<10;i++){
                cacheMmap.load();
            }
            cacheMmaps.add(cacheMmap);
        }


        //申请堆内缓存
        for (int i = 0; i < Define.heapCacheMemory / Define.memoryAllocateUnit; i++) {
            byte[] bytes=new byte[Define.memoryAllocateUnit];
            for(int j = 0; j<Define.memoryAllocateUnit /Define.cacheCapacityPerBlock; j++){
                freeMemoryBlocks.offer(new BytesCacheBlock(bytes,j*Define.cacheCapacityPerBlock));
                Define.freeCacheMemory.addAndGet(Define.cacheCapacityPerBlock);
            }
        }
        Define.printTime("allocateMemoryBlocks,size:" + 1L * freeMemoryBlocks.size() * Define.cacheCapacityPerBlock / 1024D / 1024 / 1024 + "G");
    }

    /**
     * 获取写缓存的请求对象
     *
     * @return
     * @throws Exception
     */
    public CacheRequest getWriteDatas() throws Exception {
        CacheRequest aepWriteData = freeWriteQueue.poll();
        while (aepWriteData == null) {
            aepWriteData = freeWriteQueue.poll();
            Thread.sleep(1);
        }
        return aepWriteData;
    }

    /**
     * 异步写缓存
     *
     * @param cacheRequest
     */
    public void asynWriteDatas(CacheRequest cacheRequest) {
        wrokWriteQueue.offer(cacheRequest);
    }

    /**
     * 同步写缓存
     *
     * @param cacheRequest
     */
    public void synWriteDatas(CacheRequest cacheRequest) {
        writeLock.lock();
        dealWriteRequest(cacheRequest);
        writeLock.unlock();
    }


    /**
     * 当没有空余的缓存块时，可对那些没有写满的缓存块进行共享使用，使用前需要先根据余量进行分组
     */
    private void computeSharedMemory() {
        int counter = 1;//从1开始，是避免为0时，<<32没效果，就无法通过position区分出是否是共享内存
        for (TopicCacheContext[] array : topicContexts) {
            for (TopicCacheContext context : array) {
                if (context.curBlock == null) {
                    continue;
                }
                int idx = (Define.cacheCapacityPerBlock - context.curPosition) / 1024;
                idx = idx > Define.shareCacheMaxState ? Define.shareCacheMaxState : idx;
                if (idx > 0) {//1kb打底，小了效益也不高
                    sharedBlocks[counter] = context.curBlock;
                    context.curBlock.sharedIdx = counter++;//共享块，后面不会回收
                    sharedContextMaps[idx].put(context,1);
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                CacheRequest cacheRequest;
                while ((cacheRequest = wrokWriteQueue.poll()) == null) {
                    Thread.sleep(1);
                }
                dealWriteRequest(cacheRequest);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    public void dealWriteRequest(CacheRequest cacheRequest) {
        for (int i = 0; i < cacheRequest.count; i++) {
            int dataLength = cacheRequest.dataLengthes[i];
            TopicCacheContext context = topicContexts[cacheRequest.topics[i]][cacheRequest.queueIds[i]];
            int curPosition = context.curPosition;
            int prePosition = 0;
            CacheBlock preBlock = null;
            CacheBlock curBlock = context.curBlock;
            if (curPosition + dataLength > Define.cacheCapacityPerBlock) {//写不下了
                if (curPosition < Define.cacheCapacityPerBlock) {//排除刚好写满的情况
                    preBlock = context.curBlock;//记录上一块
                    prePosition = curPosition;//记录上一块的指针位置
                }
                curBlock = null;
            }
            if (curBlock == null) {//首次，或者余量不足，需要新申请
                curBlock = freeMemoryBlocks.poll();
                if (curBlock == null) {
                    curBlock = freeAepBlocks.poll();
                    if (curBlock == null) {
                        if (!cacheFulled) {
                            cacheFulled = true;
                            computeSharedMemory();
                        } else if (!hasSharedMemory) {//地主家也没余粮了
                            continue;
                        }


                        int shareIdx = dataLength / 1024;//以kb为单位，找到对应的缓存余量map下标
                        if (dataLength % 1024 != 0) {//向上取整，比如当前写入大小为1.5kb，那就需要找余量至少2kb的共享块进行写入
                            shareIdx++;
                        }
//                        suffixLock.lock();
                        //寻找共享块，如果当前级别共享块没有了，就继续往上面找，比如当前数据是2.5kb，但是没有3kb的余量块了，就继续找4kb的、5kb的
                        while (sharedContextMaps[shareIdx].size() == 0) {
                            if (++shareIdx == sharedContextMaps.length) {
                                break;
                            }
                        }
                        if (shareIdx == sharedContextMaps.length) {//地主家开始没余粮了
//                            suffixLock.unlock();
                            hasSharedMemory = false;
                            continue;
                        }
                        //使用共享内存
                        TopicCacheContext tac = sharedContextMaps[shareIdx].keySet().iterator().next();
                        tac.curBlock.putData(cacheRequest.datas[i], 0, tac.curPosition, dataLength);
                        //高位为共享单元的下标，低位为共享单元内的偏移量,为什么没不用两个字段存? 为了原子性,避免极端情况下的线程安全问题
                        //如果是部分缓存，就将地位减去Integer.MAX_VALUE变成一个负数作为区分，比起加一个字段去标识，更节省内存，并且也能避免上述的线程安全的问题
                        cacheRequest.metas[i].cachePosition = (((long) tac.curBlock.sharedIdx << 32))
                                + (tac.curPosition-(cacheRequest.partCache[i]?Integer.MAX_VALUE:0));

                        tac.curPosition += dataLength;
                        int newShareIdx = (Define.cacheCapacityPerBlock - tac.curPosition) / 1024;
                        newShareIdx = newShareIdx > Define.shareCacheMaxState ? Define.shareCacheMaxState : newShareIdx;
                        if (newShareIdx != shareIdx) {//余量级别出现变化
                            sharedContextMaps[shareIdx].remove(tac);//从原有的map中移除
                            if (newShareIdx != 0) {//余量大于1k
                                sharedContextMaps[newShareIdx].put(tac,1);//放入新的当前余量对应的map中
                            }
                        }
//                        suffixLock.unlock();
                        continue;
                    }
                    context.curBlock = curBlock;
                } else {
                    context.curBlock = curBlock;
                }
                Define.freeCacheMemory.addAndGet(-Define.cacheCapacityPerBlock);
                curPosition = context.curPosition = 0;
                context.memoryBlocks.add(context.curBlock);
            }

            long position = (context.memoryBlocks.size() - 1) * Define.cacheCapacityPerBlock + curPosition;
            if (preBlock != null) {//跨块
                if (preBlock.sharedIdx != -1) {//如果跨块了，说明上一块肯定会被写满了，就没余量可共享了，所以需要从共享块中移除
                    sharedContextMaps[(Define.cacheCapacityPerBlock - prePosition) / 1024].remove(context);
                }

                int preLength = Define.cacheCapacityPerBlock - prePosition;//上一块的余量
                position -= preLength;//起始位置在上一块上面，所以需要减去上一块的余量，得到本次写入实际的cachePosition
                preBlock.putData(cacheRequest.datas[i], 0, prePosition, preLength);
                context.curBlock.putData(cacheRequest.datas[i], preLength, 0, dataLength - preLength);
                context.curPosition += dataLength - preLength;
            } else {
                if (context.curBlock.sharedIdx != -1) {
//                    suffixLock.lock();
                    int preIdx = (Define.cacheCapacityPerBlock - curPosition) / 1024;
                    preIdx = preIdx > Define.shareCacheMaxState ? Define.shareCacheMaxState : preIdx;
                    int curIdx = (Define.cacheCapacityPerBlock - curPosition - dataLength) / 1024;
                    curIdx = curIdx > Define.shareCacheMaxState ? Define.shareCacheMaxState : curIdx;
                    if (preIdx != curIdx) {//余量跨级
                        sharedContextMaps[preIdx].remove(context);//从原级别中移除
                        if (curIdx != 0) {//余量大于1kb,放入到对应的余量map中
                            sharedContextMaps[curIdx].put(context,1);
                        }
                    }
//                    suffixLock.unlock();
                }
                context.curBlock.putData(cacheRequest.datas[i], 0, curPosition, dataLength);
                context.curPosition += dataLength;
            }
            cacheRequest.metas[i].cachePosition =position-(cacheRequest.partCache[i]?Integer.MAX_VALUE:0);
        }
        freeWriteQueue.offer(cacheRequest);
    }

    /**
     * 批量读缓存数据，如果一次需要读取的块在缓存上是连续的，可以批量加载，减少aep访问次数
     *
     * @param topicIdx
     * @param queueId
     * @param dataMeta
     * @param aepIdxes
     * @param bytes
     */
    public void batchReadData(int topicIdx, int queueId, MQService.DataMeta dataMeta, List<Integer> aepIdxes, byte[] bytes) {
        TopicCacheContext context = topicContexts[topicIdx][queueId];
        long totalLength = 0;
        for (int idx : aepIdxes) {
            totalLength += dataMeta.metas.get(idx).dataLength;
        }
        MQService.MetaItem fst = dataMeta.metas.get(aepIdxes.get(0));
        long position = fst.cachePosition;
        int blockIdx = (int) (position / Define.cacheCapacityPerBlock);
        position = position % Define.cacheCapacityPerBlock;

        int bytesOffset = 0;
        while (position + totalLength > Define.cacheCapacityPerBlock) {
            CacheBlock cacheBlock = context.memoryBlocks.get(blockIdx);
            int length = (int) (Define.cacheCapacityPerBlock - position);
            cacheBlock.getData(bytes, bytesOffset, (int) position, length);
            bytesOffset += length;
            totalLength -= length;
            position = 0;
            blockIdx++;
        }
        CacheBlock cacheBlock = context.memoryBlocks.get(blockIdx);
        cacheBlock.getData(bytes, bytesOffset, (int) position, (int) totalLength);

    }


    /**
     * 读取缓存数据
     *
     * @param topicIdx
     * @param queueId
     * @param position
     * @param bytes
     * @param bytesOffset
     * @param dataLength
     */
    public void readData(int topicIdx, int queueId, long position, byte[] bytes, int bytesOffset, int dataLength) {
        if (position > Integer.MAX_VALUE) {//共享内存
            CacheBlock cacheBlock = sharedBlocks[(int) (position >> 32)];//找到共享块
            position=(int) position;
            if(position<0){//负数为“部分缓存”的标识
                position+=Integer.MAX_VALUE;
            }
            cacheBlock.getData(bytes, bytesOffset, (int) position, dataLength);
            return;
        }

        if(position<0){//负数为“部分缓存”的标识
            position+=Integer.MAX_VALUE;
        }

        TopicCacheContext context = topicContexts[topicIdx][queueId];

        int blockIdx = (int) (position / Define.cacheCapacityPerBlock);
        position = position % Define.cacheCapacityPerBlock;
        CacheBlock cacheBlock = context.memoryBlocks.get(blockIdx);
        if (position + dataLength > Define.cacheCapacityPerBlock) {//跨块
            int preLength = (int) (Define.cacheCapacityPerBlock - position);
            cacheBlock.getData(bytes, bytesOffset, (int) position, preLength);
            cacheBlock = context.memoryBlocks.get(blockIdx + 1);
            cacheBlock.getData(bytes, bytesOffset + preLength, 0, dataLength - preLength);
        } else {
            cacheBlock.getData(bytes, bytesOffset, (int) position, dataLength);
        }

       if(!Define.partCacheEnable){//非“部分缓存”模式，消费完即可释放缓存
           while (blockIdx != 0 && context.memoryBlocks.get(blockIdx - 1) != null) {//释放前面的块
               if (context.memoryBlocks.get(blockIdx - 1).sharedIdx != -1) {//共享内存块，不能释放
                   blockIdx--;
                   continue;
               }
               if (context.memoryBlocks.get(blockIdx - 1) instanceof AepCacheBlock) {
                   freeAepBlocks.offer(context.memoryBlocks.get(blockIdx - 1));
               } else {
                   freeMemoryBlocks.offer(context.memoryBlocks.get(blockIdx - 1));
               }
               Define.freeCacheMemory.addAndGet(Define.cacheCapacityPerBlock);
               context.memoryBlocks.set(blockIdx - 1, null);
               blockIdx--;
           }
       }
    }

    /**
     * 释放当前position之前的所有缓存块
     * @param topicIdx
     * @param queueId
     * @param position
     */
    public void freeMemoryBlocks(int topicIdx, int queueId,long position){
        int blockIdx=(int)(position/Define.cacheCapacityPerBlock);
        TopicCacheContext context = topicContexts[topicIdx][queueId];
        while (blockIdx > 0 && context.memoryBlocks.get(blockIdx - 1) != null) {//释放前面的块
            if (context.memoryBlocks.get(blockIdx - 1).sharedIdx != -1) {//共享内存块，不能释放
                blockIdx--;
                continue;
            }
            if (context.memoryBlocks.get(blockIdx - 1) instanceof AepCacheBlock) {
                freeAepBlocks.offer(context.memoryBlocks.get(blockIdx - 1));
            } else {
                freeMemoryBlocks.offer(context.memoryBlocks.get(blockIdx - 1));
            }
            context.memoryBlocks.set(blockIdx - 1, null);
            Define.freeCacheMemory.addAndGet(Define.cacheCapacityPerBlock);
            blockIdx--;
        }
    }

    /**
     * 4k缓存页，效益不高，没用上
     */
    ReentrantLock diskCacheLock=new ReentrantLock();
    @Deprecated
    public void cacheDiskDataFourK(byte fileIdx,byte[] diskDatas,long startFilePosition,int totalLength){
        diskCacheLock.lock();
        if(diskDataCaches.curBlock==null){
            diskDataCaches.curBlock=freeAepBlocks.poll();
            if(diskDataCaches.curBlock==null){
                diskCacheLock.unlock();
                return;
            }else{
                diskDataCaches.curPosition=0;
                diskDataCaches.memoryBlocks.add(diskDataCaches.curBlock);
            }
        }
        diskDataCaches.curBlock.putData(diskDatas,0,diskDataCaches.curPosition,Define.pageSize);
        diskDataMap.put((((long)fileIdx)<<32)+startFilePosition/Define.pageSize,diskDataCaches.memoryBlocks.size()*Define.memoryAllocateUnit +diskDataCaches.curPosition);
        diskDataCaches.curPosition+=Define.pageSize;
        if(diskDataCaches.curPosition==Define.cacheCapacityPerBlock){//写满了
            diskDataCaches.curBlock=freeAepBlocks.poll();
            if(diskDataCaches.curBlock==null){
                diskCacheLock.unlock();
                return;
            }else{
                diskDataCaches.memoryBlocks.add(diskDataCaches.curBlock);
                diskDataCaches.curPosition=0;
            }
        }
        diskDataCaches.curBlock.putData(diskDatas,totalLength-Define.pageSize,diskDataCaches.curPosition,Define.pageSize);
        diskDataMap.put((((long)fileIdx)<<32)+(startFilePosition+totalLength)/Define.pageSize,diskDataCaches.memoryBlocks.size()*Define.memoryAllocateUnit +diskDataCaches.curPosition);
        diskDataCaches.curPosition+=Define.pageSize;
        diskCacheLock.unlock();
    }
}
