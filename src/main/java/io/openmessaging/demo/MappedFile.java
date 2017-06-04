package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import sun.nio.ch.FileChannelImpl;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Created by KDF5000 on 2017/5/29.
 */
public class MappedFile {
    private static final int MEM_BUFFER_SIZE = 8 * 1024 * 1024; //8M
    private String indexFilePath;
    private String dataFilePath;
    private String storePath;
    private String bucket; //topic或者queue名字
    private int type; //0:topic , 1:queue

    private MappedByteBuffer indexMem;
    private long indexMemStart;
    private FileChannel indexFileChannel;

    private MappedByteBuffer dataMem;
    private long dataMemStart;
    private FileChannel dataFileChannel;

    private long currentOffset; //当前写入或者读取的位置,消息偏移
    private long indexCurrentPosition; //索引文件当前位置
    private long dataCurrentPostion; //数据文件当前位置

    public MappedFile(String storePath, String bucketName, int type){
        this.storePath = storePath;
        this.bucket = bucketName;
        this.indexFilePath = storePath+"/"+type+"_"+bucket+".index";
        this.dataFilePath = storePath+"/"+type+"_"+bucket+".data";
        this.currentOffset = 0;
        this.indexCurrentPosition = 0;
        this.dataCurrentPostion = 0;
        this.indexMemStart = 0;
        this.dataMemStart = 0;
        try{
            initMappedFile();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void cleanBuffer(final Object buffer) throws Exception {
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner",new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
                    cleaner.clean();
                } catch(Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

//    private void unmap(MappedByteBuffer buffer) {
//        try{
//            Method m = FileChannelImpl.class.getDeclaredMethod("unmap",
//                    MappedByteBuffer.class);
//            m.setAccessible(true);
//            m.invoke(FileChannelImpl.class, buffer);
//
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }

    private void initMappedFile() throws FileNotFoundException,IOException{
        File indexFile = new File(indexFilePath);
        File dataFile = new File(dataFilePath);
        if(!indexFile.exists()){
            indexFile.createNewFile();
        }
        if(!dataFile.exists()){
            dataFile.createNewFile();
        }
        indexFileChannel = new RandomAccessFile(indexFile,"rw").getChannel();
        indexMem = indexFileChannel.map(FileChannel.MapMode.READ_WRITE,indexMemStart,MEM_BUFFER_SIZE);

        dataFileChannel = new RandomAccessFile(dataFile,"rw").getChannel();
        dataMem = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,dataMemStart,MEM_BUFFER_SIZE*2);
    }

    public void putMessage(Message msg) throws IOException{
//        byte[] msgBytes = SerializeUtil.serialize(msg);
//        byte[] msgBytes = SerializeUtil.serializeMessage(msg);
        byte[] msgBytes = SerializeUtil.serializeMessageFull(msg);
        int msgLen = msgBytes.length;
//        if(msgLen>400){
//            System.out.println("[KDF5000] Len:"+msgLen);
//        }

        if(!indexMem.hasRemaining()){
            indexMem.force();
            //先释放改缓冲区
//            unmap(indexMem);
            try{
                cleanBuffer(indexMem);
            }catch (Exception e){
                //
            }
            indexMemStart += indexMem.position();
            indexMem = indexFileChannel.map(FileChannel.MapMode.READ_WRITE,indexMemStart,MEM_BUFFER_SIZE);
        }
        if(!dataMem.hasRemaining() || (MEM_BUFFER_SIZE*2 - dataMem.position()) < msgLen+4){
            dataMem.force();
            //先释放改缓冲区
//            unmap(dataMem);
            try{
                cleanBuffer(dataMem);
            }catch (Exception e){
                //
            }
            dataMemStart += dataMem.position();
            dataMem = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,dataMemStart, MEM_BUFFER_SIZE*2);
        }
        //放置16个字节
        indexMem.putLong(++currentOffset);
        indexMem.putLong(dataCurrentPostion);
        //放置4+message个字节
        dataMem.putInt(msgLen);
        dataMem.put(msgBytes);
        dataCurrentPostion += 4+msgLen;
    }

    public synchronized Message getMessage(long offset) throws IOException{
        //索引文件没有这么多内容
        if(offset*16 > indexFileChannel.size()){
            return null;
        }
//        System.out.println(indexFileChannel.size());
        //在当前indexMem
        if((offset-1)*16 < indexMemStart || (offset-1)*16 > indexMemStart+MEM_BUFFER_SIZE-16){
//            unmap(indexMem);
            try{
                cleanBuffer(indexMem);
            }catch (Exception e){
                //
            }
            indexMemStart = (offset-1)*16;
//            indexMem.force();
            indexMem = indexFileChannel.map(FileChannel.MapMode.READ_ONLY, indexMemStart,MEM_BUFFER_SIZE);
        }
//        System.out.println(indexMemStart);
        Long indexStart = (offset-1)*16 -indexMemStart;
        long indexOffset = indexMem.getLong(indexStart.intValue());
//        System.out.println(indexOffset);
        if (indexOffset != offset){
            return null;
        }
        long dataOffeset = indexMem.getLong(indexStart.intValue()+8);
//        System.out.println(dataOffeset);

        if(dataOffeset < dataMemStart || dataOffeset > dataMemStart+2*MEM_BUFFER_SIZE - 4*1024 ){//还剩4k就加载
//            unmap(dataMem);
            try{
                cleanBuffer(dataMem);
            }catch (Exception e){
                //
            }
            dataMemStart = dataOffeset;
            //强制刷新
//            dataMem.force();
            dataMem = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, dataOffeset, 2* MEM_BUFFER_SIZE);
        }
        int dataStart = new Long(dataOffeset-dataMemStart).intValue();
//        System.out.println("dataStart:"+dataStart+",datamemSize:"+dataMem.capacity());
        int msgLen = dataMem.getInt(dataStart);
//        System.out.println(msgLen);
//        System.out.println(bucket+"Offset:"+offset+",indexMemStart"+indexMemStart+",Len:"+msgLen+"indexStart:"+indexStart+",dataOffeset:"+dataOffeset+",dataMemStart:"+dataMemStart);
        byte []data = new byte[msgLen];

        dataMem.get(dataStart);
        int j = 0;

        for (int i=dataStart+4;i<dataStart+4+msgLen;i++){
            data[j++] = dataMem.get(i);
//            System.out.println(Integer.toHexString(data[j-1]));
        }
//        Message msg = (Message) SerializeUtil.unserialize(data);
//        Message msg = (Message)SerializeUtil.unserializeMessage(data);
        Message msg = (Message)SerializeUtil.unserializeMessageFull(data);
        if(msg != null){
            return msg;
        }

        return null;
    }


    public static void main(String []args) throws IOException{
        MappedFile mapFile =new MappedFile(Constant.STORE_PATH,"QUEUE_0", Constant.TYPE_QUEUE);
        for(int i=1;;i++){
            DefaultBytesMessage msg = (DefaultBytesMessage)mapFile.getMessage(i);
            if(msg!=null){
                System.out.println("Header: "+ msg.headers().getString(MessageHeader.QUEUE) + ",Body: "+new String(msg.getBody()));
            }else{
                System.out.println("Total Message: "+i);
                break;
            }
        }

    }
}
