package io.openmessaging.ps.cache.block;

import io.openmessaging.common.ResourceManager;
import sun.misc.Unsafe;

/**
 * 缓存块基类
 * @author chender
 * @date 2021/10/12 11:21
 */
public abstract class CacheBlock {
    static Unsafe unsafe= ResourceManager.getUnsafe();

    /**
     * 共享缓存标识，如果不为-1，代表该缓存块被共享了
     */
    public int sharedIdx=-1;

    /**
     * 往缓存块写入数据
     * @param data
     * @param dataOffset
     * @param myOffset
     * @param length
     */
    public abstract void putData(byte[] data, int dataOffset, int myOffset, int length);

    /**
     * 从缓存块读取数据
     * @param data
     * @param dataOffset
     * @param myOffset
     * @param length
     */
    public abstract void getData(byte[] data,int dataOffset,int myOffset,int length);
}
