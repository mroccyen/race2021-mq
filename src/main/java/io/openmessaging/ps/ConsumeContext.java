package io.openmessaging.ps;

import io.openmessaging.common.Define;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * 读数据上线下文，便于复用资源
 *
 * @author chender
 * @date 2021/9/12 10:48
 */
public class ConsumeContext {
    private static AtomicInteger squence=new AtomicInteger();
    Map<Integer, ByteBuffer> results;
    ByteBuffer[] byteBuffers;//用于存储读取到的数据的heapByteBuffer
//     byte[] bytes;
    byte[][] bytes;
    FileChannel[] channels;
    public ConsumeContext(){
        results=new HashMap<>(200);
        byteBuffers=new ByteBuffer[Define.maxFetchCount];
        bytes=new byte[Define.maxFetchCount][];
        Field field=null;
//         Field capacityField=null;
        //byteBuffers们底层是一个大数组，这样这些buffer在内存上就是连续的了，在批量使用缓存的时候，就能减少copy次数了（这个逻辑有间歇性bug，注释了）
        try{
            field=ByteBuffer.class.getDeclaredField("hb");
//            capacityField= Buffer.class.getDeclaredField("capacity");
            field.setAccessible(true);
//            capacityField.setAccessible(true);
//            bytes=new byte[Define.maxDataLength*Define.maxFetchCount];
            for(int i=0;i<Define.maxFetchCount;i++){
                byteBuffers[i]=ByteBuffer.allocate(Define.maxDataLength);
                bytes[i]=(byte[])field.get(byteBuffers[i]);
//             field.set(byteBuffers[i],bytes);
//             capacityField.set(byteBuffers[i],Define.maxDataLength*Define.maxFetchCount);
            }
        }catch (Exception e){
            System.out.println("reflect set bytes fail");
            e.printStackTrace();
            System.exit(-1);
        }
        channels=new FileChannel[Define.psThreads*2];
        try {
            for(int i=0;i<Define.psThreads;i++){
                File file=new File(Define.ESSDPATH+File.separator+i+".data");
                channels[i]=FileChannel.open(file.toPath(), READ);
                //磁盘读取会存在双线程读取的情况，所以需要需要两个channel
                channels[i+Define.psThreads]=FileChannel.open(file.toPath(), READ);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
