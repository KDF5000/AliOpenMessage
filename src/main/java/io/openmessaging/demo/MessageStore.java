package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

class MessageFlush implements Runnable{
    private ArrayBlockingQueue<Message> queue;
    private HashMap<String, MappedFile> mmapFileMap = new HashMap<String,MappedFile>();
    private String storePath;
    public MessageFlush(ArrayBlockingQueue queue,String storePath){
        this.queue = queue;
        this.storePath = storePath;
    }
    @Override public void run(){
        try{
            int count = 0;
            while(true){
                if(queue.isEmpty() && count>0){
                    break;
                }
                Message message = queue.take();
                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                if ((topic == null && queue == null) || (topic != null && queue != null)) {
                    throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
                }
                String bucket = topic != null ? topic : queue;
                int type = topic!=null ? Constant.TYPE_TOPIC : Constant.TYPE_QUEUE;

                MappedFile mmapFile = null;
                if(mmapFileMap.containsKey(type+bucket)){
                    mmapFile = mmapFileMap.get(type+bucket);
                }else{
                    mmapFile = new MappedFile(this.storePath,bucket,type);
                    mmapFileMap.put(type+bucket,mmapFile);
                }
                try{
                    mmapFile.putMessage(message);
                }catch (IOException e){
                    e.printStackTrace();
                }
                count++;
                if(count%1000000==0){
                    System.out.println("[KDF5000] count:"+count);
                }
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}


public class MessageStore {
    private static int MESSAGE_QUEUE_LEN = 1000000;

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private ArrayBlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(MESSAGE_QUEUE_LEN);
    private HashMap<String, MappedFile> mmapFileMap = new HashMap<String,MappedFile>();
    private boolean isFlushing = false;

    public MessageStore(){
        //启动刷新消息线程
    }


    public void startFlushDisk(String storePath){
        if(isFlushing){
            return;
        }
        if(storePath==null){
            storePath = Constant.STORE_PATH;
        }
        Thread flushThread = new Thread(new MessageFlush(queue,storePath));
        flushThread.start();
        isFlushing=true;
    }
    //synchronized
    public void putMessage(String bucket, Message message) {
//        if (!messageBuckets.containsKey(bucket)) {
//            messageBuckets.put(bucket, new ArrayList<>(1024));
//        }
//        ArrayList<Message> bucketList = messageBuckets.get(bucket);
//        bucketList.add(message);
        //放入队列，等待异步落盘
        try{
            queue.put(message);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
    //synchronized
    public synchronized Message pullMessage(String bucket,int type, long offset,String storePath){
        MappedFile mmapFile = null;
        if(mmapFileMap.containsKey(type+bucket)){
            mmapFile = mmapFileMap.get(type+bucket);
        }else{
            mmapFile = new MappedFile(storePath,bucket,type);
            mmapFileMap.put(type+bucket,mmapFile);
        }
        try{
            Message msg = mmapFile.getMessage(offset);
            return msg;
        }catch (Exception e){
//            e.printStackTrace();
        }
        return null;
    }

   public synchronized Message pullMessage(String queue, String bucket) {
//        System.out.printf("%s %s\n",queue,bucket);
       ArrayList<Message> bucketList = messageBuckets.get(bucket);
       if (bucketList == null) {
           return null;
       }
       HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
       if (offsetMap == null) {
           offsetMap = new HashMap<>();
           queueOffsets.put(queue, offsetMap);
       }
       int offset = offsetMap.getOrDefault(bucket, 0);
       if (offset >= bucketList.size()) {
           return null;
       }
       Message message = bucketList.get(offset);
       offsetMap.put(bucket, ++offset);
       return message;
   }
}
