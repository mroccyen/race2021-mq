package io.openmessaging.ps.cache;

import io.openmessaging.common.Define;
import io.openmessaging.ps.MQService;

/**
 * 写缓存请求(一批聚合的数据会放到一个cacheRequest里面)
 * @author chender
 * @date 2021/10/3 15:35
 */
public class CacheRequest {
    //..原谅我用public吧，真的就是为了图个方便
//    public int fileIdx;//原始数据的文件下标
//    public long diskPosition;//原始数据的磁盘位置
    public int[] topics;
    public int[] queueIds;
    public byte[][] datas;//待写入缓存的数据们
    public int[] dataLengthes; //每块数据的长度
    public MQService.MetaItem[] metas;//每块数据对应的meta数据
    public boolean[] partCache;//是否为部分缓存(只缓存了某块数据的某一部分)
    public int count;//聚合数据块的数量

    public CacheRequest(){
        this.topics=new int[Define.maxForceJoinCount];
        this.queueIds=new int[Define.maxForceJoinCount];
        this.datas=new byte[Define.maxForceJoinCount][Define.maxDataLength];
        this.dataLengthes=new int[Define.maxForceJoinCount];
        metas =new MQService.MetaItem[Define.maxForceJoinCount];
        partCache=new boolean[Define.maxForceJoinCount];
    }
}
