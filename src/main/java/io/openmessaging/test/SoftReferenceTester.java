package io.openmessaging.test;

import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * @author chender
 * @date 2021/10/30 23:24
 */
public class SoftReferenceTester {

    public static void main(String[] args)throws Exception {
//        SoftReference<byte[]> sr=new SoftReference<>(new byte[128*1024*1024]);
//        List<byte[]> list=new ArrayList<>();
//        for(int i=0;i<15;i++){
//            list.add(new byte[128*1024*1024]);
//        }
//        System.out.println(list.size());
//
//
//        System.out.println(sr.get());


        FileChannel fileChannel=FileChannel.open(new File("d:/1.sss").toPath(),WRITE,CREATE,READ);
        MappedByteBuffer mappedByteBuffer=fileChannel.map(FileChannel.MapMode.READ_WRITE,0,1024*1024*1024);
//        mappedByteBuffer.load();


        long start=System.currentTimeMillis();
        ByteBuffer byteBuffer=ByteBuffer.allocate(1024*1024);
        for(int i=0;i<1024;i++){
            byteBuffer.clear();
            mappedByteBuffer.put(byteBuffer);
        }
        System.out.println(System.currentTimeMillis()-start);

        byte[] bytes=new byte[1024*1024];
        start=System.currentTimeMillis();
        mappedByteBuffer.clear();
        for(int i=0;i<1024;i++){
//            byteBuffer.clear();
            mappedByteBuffer.get(bytes,0,bytes.length);
        }
        System.out.println(System.currentTimeMillis()-start);

        List<MappedByteBuffer> list=new ArrayList<>();
        for(int i=0;i<10;i++){
            File f=new File("d:/1.sss"+i);
            FileChannel fc=FileChannel.open(f.toPath(),WRITE,CREATE,READ);
            MappedByteBuffer m=fc.map(FileChannel.MapMode.PRIVATE,0,1024*1024*1024);
//            list.add(m);

            for(int j=0;j<1024;j++){
                byteBuffer.clear();
                m.put(byteBuffer);
            }
            fc.close();
//            ((DirectBuffer)m).cleaner().clean();

            f.delete();

        }


        start=System.currentTimeMillis();
        mappedByteBuffer.clear();
        for(int i=0;i<1024;i++){
//            byteBuffer.clear();
            mappedByteBuffer.get(bytes,0,bytes.length);
        }
        System.out.println(System.currentTimeMillis()-start);
    }
}
