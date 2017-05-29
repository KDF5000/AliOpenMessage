package io.messaging.demo;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by KDF5000 on 2017/5/7.
 */
//public class MmapDemo {
//    private static int count = 10485760;
//
//    public static void main(String[] args) throws  Exception{
//        RandomAccessFile mmMapperFile = new RandomAccessFile("demo.txt", "rw");
//        //map 文件到内存
//        MappedByteBuffer out = mmMapperFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, count);
//
//        for (int i = 0;i < count;i++){
//            out.put((byte)'A');
//        }
//        System.out.println("写文件成功!");
//
//        for (int i=0;i<count;i++){
//            System.out.println((char)out.get(i));
//        }
//
//        System.out.println("读取完成!");
//        mmMapperFile.close();
//    }
//}

public class MmapDemo{
    private static int BUFFER_SIZE = 8*1000;//8M

    public static void write()throws FileNotFoundException,IOException{
        RandomAccessFile f = new RandomAccessFile("mmaped.txt","rw");

        FileChannel fc = f.getChannel();
        MappedByteBuffer mem = fc.map(FileChannel.MapMode.READ_WRITE,0,BUFFER_SIZE);

        int start = 0;
        long counter = 1;
        long HUNDREDK = 100000;
        long startT = System.currentTimeMillis();
        long noOfMessage = HUNDREDK * 10 *10;
        for(;;){
            if (!mem.hasRemaining()){
                start += mem.position();
                System.out.println(start);
                mem = fc.map(FileChannel.MapMode.READ_WRITE, start, BUFFER_SIZE);
            }
            System.out.println(mem.position());
            mem.putLong(counter++);
            if(counter> noOfMessage){
                break;
            }
        }
        long endT = System.currentTimeMillis();
        long tot = endT - startT;
        System.out.println(String.format("No of Message %s, Time(ms) %s",noOfMessage, tot));
    }

    public static void read() throws FileNotFoundException,IOException{
        FileChannel fc = new RandomAccessFile("mmaped.txt", "rw").getChannel();
        MappedByteBuffer mem = fc.map(FileChannel.MapMode.READ_ONLY,0, BUFFER_SIZE);
        long oldSize = fc.size();
        System.out.println("Fc size:"+oldSize);
        long currentPos = 0;
        long xx = currentPos;

        long startT = System.currentTimeMillis();
        long lastValue = -1;
        for(;;){
            while(mem.hasRemaining()){
                lastValue = mem.getLong();
//                System.out.println("Mpos:"+mem.position());
//                System.out.println("LastValue:"+lastValue);
                currentPos +=8;
            }
//            System.out.println("CurrentPos:"+currentPos);
            if (currentPos < oldSize){
                xx = xx + mem.position();
                mem = fc.map(FileChannel.MapMode.READ_ONLY, xx, BUFFER_SIZE);
                continue;
            }else{
                //已经读完
                long endT = System.currentTimeMillis();
                long tot = endT- startT;
                System.out.println(String.format("Last Value Read %s, Time(ms):%s", lastValue,tot));
                break;
                //等到新数据
//                while(true){
//                    long newSize = fc.size();
//                    if (newSize > oldSize){
//                        oldSize = newSize;
//                        xx = xx + mem.position();
//                        mem = fc.map(FileChannel.MapMode.READ_ONLY, xx, oldSize-xx);
//                        System.out.println("Got some data");
//                        break;
//                    }
//                }
            }
        }

    }
    public static void main(String []args) throws  IOException{
        write();
        read();
    }
}