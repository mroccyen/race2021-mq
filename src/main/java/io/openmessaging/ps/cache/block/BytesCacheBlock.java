package io.openmessaging.ps.cache.block;

/**
 * 基于byte数组的缓存块
 * @author chender
 * @date 2021/10/12 23:22
 */
public class BytesCacheBlock extends CacheBlock {
    private byte[] bytes;
    private int offset;
    public BytesCacheBlock(byte[] bytes,int offset){
        this.bytes=bytes;
        this.offset=offset;
    }

    @Override
    public void putData(byte[] data, int dataOffset, int myOffset, int length) {
      System.arraycopy(data,dataOffset,bytes,offset+myOffset,length);
    }

    @Override
    public void getData(byte[] data, int dataOffset, int myOffset, int length) {
        System.arraycopy(bytes,offset+myOffset,data,dataOffset,length);
    }
}
