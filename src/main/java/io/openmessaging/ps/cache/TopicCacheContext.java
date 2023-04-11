package io.openmessaging.ps.cache;

import io.openmessaging.ps.cache.block.CacheBlock;

import java.util.ArrayList;
import java.util.List;

/**
 * 缓存上下文，以topic+queueId为单位
 * @author chender
 * @date 2021/9/30 15:27
 */
public class TopicCacheContext {
    //..原谅我用public吧，真的就是为了图个方便
    public List<CacheBlock> memoryBlocks;//缓存块列表
    public CacheBlock curBlock;//当前最新缓存块
    public int curPosition;//当前最新缓存块的写入指针位置

    public TopicCacheContext(){
        memoryBlocks=new ArrayList<>();
    }
}
