package io.openmessaging.demo;

import io.openmessaging.*;

import java.util.concurrent.ArrayBlockingQueue;

public class DefaultProducer  implements Producer {

    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        //增加计数器
        this.messageStore.producerUp();
        this.messageStore.startFlushDisk(properties.getString("STORE_PATH"));
    }

    @Override public void flush(){
//        System.out.println("[KDF5000] begin to flush!");
//        this.messageStore.startFlushDisk(properties.getString("STORE_PATH"));
        this.messageStore.producerDown();
        if(this.messageStore.getFlushStatus()){
            try{
//                System.out.println("[KDF5000] wait flush threads finished!");
                this.messageStore.waitFlush();
            }catch (Exception e){
                //do noting
            }
        }
    }
    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
//        System.out.println("[KDF5000] send a message!");
        messageStore.putMessage(topic != null ? topic : queue, message);
    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
}
