package io.messaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;

class MessageFlush implements Runnable{
    private ArrayBlockingQueue<Message> queue;
    private HashMap<String, MappedFile> mmapFileMap = new HashMap<String,MappedFile>();


    public MessageFlush(ArrayBlockingQueue queue){ this.queue = queue; }
    @Override public void run(){
        try{
            while(true){
                Message message = queue.take();
                String topic = message.headers().getString(MessageHeader.TOPIC);
                String queue = message.headers().getString(MessageHeader.QUEUE);
                if ((topic == null && queue == null) || (topic != null && queue != null)) {
                    throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
                }
                String bucket = topic != null ? topic : queue;

                MappedFile mmapFile = null;
                if(mmapFileMap.containsKey(bucket)){
                    mmapFile = mmapFileMap.get(bucket);
                }else{
                    int type = topic!=null ? Constant.TYPE_TOPIC : Constant.TYPE_QUEUE;
                    mmapFile = new MappedFile(Constant.STORE_PATH,bucket,type);
                    mmapFileMap.put(bucket,mmapFile);
                }
                try{
                    mmapFile.putMessage(message);
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}


public class MessageStore {
    private static int MESSAGE_QUEUE_LEN = 100000;

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private ArrayBlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(MESSAGE_QUEUE_LEN);

    public MessageStore(){
        //启动刷新消息线程
        Thread flushThread = new Thread(new MessageFlush(queue));
        flushThread.start();
    }

    public synchronized void putMessage(String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        bucketList.add(message);
        //放入队列，等待异步落盘
        try{
            queue.put(message);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
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