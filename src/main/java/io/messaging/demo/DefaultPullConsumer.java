package io.messaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.*;
public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private ConsumeQueue consumeQueue;

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }

//    @Override public synchronized Message poll() {
//        if (buckets.size() == 0 || queue == null) {
//            return null;
//        }
//        //use Round Robin
//        int checkNum = 0;
//        while (++checkNum <= bucketList.size()) {
//            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
//            Message message = messageStore.pullMessage(queue, bucket);
//            if (message != null) {
//                return message;
//            }
//        }
//        return null;
//    }

    @Override
    public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        Message msg = consumeQueue.pollMessage();
        if(msg != null){
            return msg;
        }
        return null;
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName) || consumeQueue!=null) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName);
        buckets.addAll(topics); //这样的效果是并没有区分queue和topic,但是queuename将作为queue的标识
        bucketList.clear();
        bucketList.addAll(buckets);
        consumeQueue = new ConsumeQueue(queueName, bucketList,properties);
    }

}
