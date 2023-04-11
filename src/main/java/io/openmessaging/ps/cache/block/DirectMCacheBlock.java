package io.openmessaging.ps.cache.block;

import io.openmessaging.common.ResourceManager;
import sun.misc.Unsafe;

/**
 * 基于堆外内存的缓存块
 * @author chender
 * @date 2021/10/12 11:22
 */
public class DirectMCacheBlock extends CacheBlock {
    private static Unsafe unsafe=ResourceManager.getUnsafe();
    private static int byteArrayBase=unsafe.arrayBaseOffset(byte[].class);
    private long address;
    public DirectMCacheBlock(long address){
        this.address=address;
    }

    @Override
    public void putData(byte[] data, int dataOffset, int myOffset, int length) {
        unsafe.copyMemory(data,byteArrayBase+dataOffset,null,address+myOffset,length);
    }

    @Override
    public void getData(byte[] data, int dataOffset, int myOffset, int length) {
        unsafe.copyMemory(null,address+myOffset,data,byteArrayBase+dataOffset,length);
    }
}
