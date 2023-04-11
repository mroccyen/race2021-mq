package io.openmessaging.ps.cache.block;

import sun.misc.Unsafe;

/**
 * 基于aep的缓存块
 * @author chender
 * @date 2021/10/12 23:21
 */
public class AepCacheBlock extends CacheBlock {
    private long baseAddress;
    public AepCacheBlock(long baseAddress){
        this.baseAddress=baseAddress;
    }

    @Override
    public void putData(byte[] data, int dataOffset, int myOffset, int length) {
        unsafe.copyMemory(data, Unsafe.ARRAY_BYTE_BASE_OFFSET+Unsafe.ARRAY_BYTE_INDEX_SCALE*dataOffset, null, baseAddress+myOffset, length);
    }

    @Override
    public void getData(byte[] data, int dataOffset, int myOffset, int length) {
        unsafe.copyMemory(null, baseAddress+myOffset, data, Unsafe.ARRAY_BYTE_BASE_OFFSET+Unsafe.ARRAY_BYTE_INDEX_SCALE*dataOffset, length);
    }
}
