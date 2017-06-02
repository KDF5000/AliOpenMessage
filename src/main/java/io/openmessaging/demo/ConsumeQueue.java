package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.originalemo.DefaultKeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by KDF5000 on 2017/5/29.
 */
public class ConsumeQueue {
    private List<String> bucketList;
    private int lastIndex; //当前从队列中获取的条数
    private String queueName; //队列名字
    private MessageStore messageStore = MessageStore.getInstance();
    private  KeyValue properties;
    private HashMap<String, Long> offsetMap = new HashMap<String, Long>();
    private String storePath;

    public ConsumeQueue(String quene, List<String> buckets, KeyValue properties){
        this.properties = properties;
        this.queueName = quene;
        this.bucketList = buckets;
        this.storePath = properties.getString("STORE_PATH");
    }

    public Message pollMessage(){
        if(queueName == null || bucketList==null || bucketList.size() == 0){
            return null;
        }
        //use Round Robin
        int checkNum = 0;
        while (++checkNum <= bucketList.size()) {
            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
            int type = bucket == queueName ? Constant.TYPE_QUEUE : Constant.TYPE_TOPIC;
//            System.out.println(type);
            long offset = 1;
            if (offsetMap.containsKey(bucket)){
                offset = offsetMap.get(bucket);
            }
//            System.out.println("Bucket:"+bucket+", Offset:"+offset);
            Message message = messageStore.pullMessage(bucket,type,offset,this.storePath);
            if (message != null) {
                offsetMap.put(bucket,++offset);
                return message;
            }
        }
        return null;
    }

    public static void main(String []args){
        List<String> topics = new ArrayList<String>();
        topics.add("TOPIC1");
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/Users/KDF5000/Documents/2017/Coding/Middleware/data");
        ConsumeQueue queue = new ConsumeQueue("QUEUE1",topics,properties);
        DefaultBytesMessage msg = (DefaultBytesMessage)queue.pollMessage();
        System.out.println("Header: "+ msg.headers().getString(MessageHeader.TOPIC) + "Body: "+new String(msg.getBody()));
    }
}
