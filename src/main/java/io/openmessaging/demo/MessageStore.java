package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

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
//                if(queue.isEmpty() && count>0){
//                    System.out.println("[KDF5000] Flush finished!");
//                    counter.incrementAndGet();
//                    break;
//                }
//                queue.take();
                Message message = queue.poll(1, TimeUnit.SECONDS);
                if(message==null && queue.size()==0){
                    Thread.sleep(1000);
                    System.out.println("[KDF5000] Flush finished!"+queue.size());
                    break;
                }
//                System.out.println(Thread.currentThread().getName()+"take:"+queue.size());
//                Message message = queue.take();
                String queue = message.headers().getString(MessageHeader.QUEUE);
                String topic = message.headers().getString(MessageHeader.TOPIC);
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
    private static int MESSAGE_QUEUE_LEN = 800000;
    private static int QUEUE_NUM = 5;
    private static final MessageStore INSTANCE = new MessageStore();
    Thread[] ts = new Thread[QUEUE_NUM];
    private static AtomicInteger poducerCounter;//记录producer结束的个数，用于停止落盘线程

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private ArrayBlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(MESSAGE_QUEUE_LEN);
    //多个队列
    private ArrayBlockingQueue<Message>[] queues = new ArrayBlockingQueue[QUEUE_NUM];

    private HashMap<String, MappedFile> mmapFileMap = new HashMap<String,MappedFile>();

    private boolean isFlushing = false;

    public MessageStore(){
        //启动刷新消息线程
        for(int i=0;i<QUEUE_NUM;i++){
            queues[i] = new ArrayBlockingQueue<Message>(MESSAGE_QUEUE_LEN);
        }
    }

    public synchronized void startFlushDisk(String storePath){
        if(isFlushing){
            return;
        }
        if(storePath==null){
            storePath = Constant.STORE_PATH;
        }
        for (int i=0;i<QUEUE_NUM;i++){
            ts[i] = new Thread(new MessageFlush(queues[i],storePath));
        }
        for(int i=0;i<QUEUE_NUM;i++){
            ts[i].start();
        }
        isFlushing = true;
    }

    public synchronized void waitFlush() throws Exception{
        for(int i=0;i<QUEUE_NUM;i++){
            ts[i].join();
        }
        System.out.println("[KDF5000] All flush threads finished!");
        isFlushing = false;
    }

    public synchronized boolean getFlushStatus() {
        return isFlushing;
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
//            queue.put(message);
            queues[Math.abs(bucket.hashCode())%QUEUE_NUM].put(message);
//            System.out.println(Math.abs(bucket.hashCode())%QUEUE_NUM + ","+queues[Math.abs(bucket.hashCode())%QUEUE_NUM].size());
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
