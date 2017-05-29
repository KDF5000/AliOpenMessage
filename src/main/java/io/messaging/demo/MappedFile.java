package io.messaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by KDF5000 on 2017/5/29.
 */
public class MappedFile {
    private static final int MEM_BUFFER_SIZE = 128 * 1024 * 1024; //128M
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
        byte[] msgBytes = SerializeUtil.serialize(msg);
        int msgLen = msgBytes.length;

        if(!indexMem.hasRemaining()){
            indexMemStart += indexMem.position();
            indexMem = indexFileChannel.map(FileChannel.MapMode.READ_WRITE,indexMemStart,MEM_BUFFER_SIZE);
        }
        if(!dataMem.hasRemaining() || (MEM_BUFFER_SIZE*2 - dataMem.position()) < msgLen+4){
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

    public Message getMessage(long offset){
        return null;
    }
}