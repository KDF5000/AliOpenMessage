package io.messaging.demo;

import io.openmessaging.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Topic{
    private int type;//0:topic, 1:queue
    private String topicName;

    public Topic(int type, String topicName){
        this.topicName = topicName;
        this.type = type;
    }

    public int getType(){ return this.type;}
    public String getTopicName(){ return this.topicName;}
}

class ProducerTester implements Runnable{
    private ArrayList<Topic> topics;
    public Producer producer;
    private KeyValue properties;
    private Random random;

    public ProducerTester(KeyValue properties,ArrayList<Topic> topics){
        this.properties = properties;
        this.topics = topics;
        this.producer = new DefaultProducer(properties);
        this.random = new Random();
    }

    @Override
    public void run() {
        while (true){
            int topicIndex = random.nextInt(topics.size());
            Topic topic = topics.get(topicIndex);

            int randeNum = random.nextInt(10000000);
            Message msg = null;
            if(topic.getType() == Constant.TYPE_QUEUE){
                msg = producer.createBytesMessageToQueue(topic.getTopicName(),(topic.getTopicName()+randeNum).getBytes() );
            }else if (topic.getType() == Constant.TYPE_TOPIC){
                msg = producer.createBytesMessageToTopic(topic.getTopicName(),(topic.getTopicName()+randeNum).getBytes() );
            }

            if(msg != null){
                producer.send(msg);
            }
        }
    }
}

public class DemoTester {

    public static void main(String[] args) throws InterruptedException {
        KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
        properties.put("STORE_PATH", "/Users/KDF5000/Documents/2017/Coding/Middleware/data");

        //构造测试数据
        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE1"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE2"; //实际测试时，queue数目与消费线程数目相同

        ArrayList<Topic> topics = new ArrayList<Topic>();
        topics.add(new Topic(Constant.TYPE_TOPIC,"TOPIC1"));
        topics.add(new Topic(Constant.TYPE_TOPIC,"TOPIC2"));
        topics.add(new Topic(Constant.TYPE_QUEUE,"QUEUE1"));
        topics.add(new Topic(Constant.TYPE_QUEUE,"QUEUE2"));

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long start = System.currentTimeMillis();
        for(int i=0;i<10;i++){
            executorService.execute(new ProducerTester(properties,topics));
        }

        Thread.sleep(2000);
        executorService.shutdown();
        long end = System.currentTimeMillis();
        long T1 = end - start;
        System.out.println("Producer Time:%s"+T1);
//
//        //请保证数据写入磁盘中
//
//        //消费样例1，实际测试时会Kill掉发送进程，另取进程进行消费
//        {
//            PullConsumer consumer1 = new DefaultPullConsumer(properties);
//            consumer1.attachQueue(queue1, Collections.singletonList(topic1));
//
//            int queue1Offset = 0, topic1Offset = 0;
//
//            long startConsumer = System.currentTimeMillis();
//            while (true) {
//                Message message = consumer1.poll();
//                if (message == null) {
//                    //拉取为null则认为消息已经拉取完毕
//                    break;
//                }
//                String topic = message.headers().getString(MessageHeader.TOPIC);
//                String queue = message.headers().getString(MessageHeader.QUEUE);
//                //实际测试时，会一一比较各个字段
//                if (topic != null) {
//                    Assert.assertEquals(topic1, topic);
//                    Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
//                } else {
//                    Assert.assertEquals(queue1, queue);
//                    Assert.assertEquals(messagesForQueue1.get(queue1Offset++), message);
//                }
//            }
//            long endConsumer = System.currentTimeMillis();
//            long T2 = endConsumer - startConsumer;
//            System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2 + T1, (queue1Offset + topic1Offset)/(T1 + T2)));
//        }
//
//        //消费样例2，实际测试时会Kill掉发送进程，另取进程进行消费
//        {
//            PullConsumer consumer2 = new DefaultPullConsumer(properties);
//            List<String> topics = new ArrayList<>();
//            topics.add(topic1);
//            topics.add(topic2);
//            consumer2.attachQueue(queue2, topics);
//
//            int queue2Offset = 0, topic1Offset = 0, topic2Offset = 0;
//
//            long startConsumer = System.currentTimeMillis();
//            while (true) {
//                Message message = consumer2.poll();
//                if (message == null) {
//                    //拉取为null则认为消息已经拉取完毕
//                    break;
//                }
//
//                String topic = message.headers().getString(MessageHeader.TOPIC);
//                String queue = message.headers().getString(MessageHeader.QUEUE);
//                //实际测试时，会一一比较各个字段
//                if (topic != null) {
//                    if (topic.equals(topic1)) {
//                        Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
//                    } else {
//                        Assert.assertEquals(topic2, topic);
//                        Assert.assertEquals(messagesForTopic2.get(topic2Offset++), message);
//                    }
//                } else {
//                    Assert.assertEquals(queue2, queue);
//                    Assert.assertEquals(messagesForQueue2.get(queue2Offset++), message);
//                }
//            }
//            long endConsumer = System.currentTimeMillis();
//            long T2 = endConsumer - startConsumer;
//            System.out.println(String.format("Team2 cost:%d ms tps:%d q/ms", T2 + T1, (queue2Offset + topic1Offset)/(T1 + T2)));
//        }
    }
}
