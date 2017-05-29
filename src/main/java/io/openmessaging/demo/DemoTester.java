package io.openmessaging.demo;

import io.openmessaging.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        int count=0;
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
            count++;
            if(count>200000){
                break;
            }
        }
        producer.flush();
    }
}

class ConsumerTester implements Runnable{
    private ArrayList<String> topics;
    public PullConsumer consumer;
    private KeyValue properties;
    private String queueName;

    public ConsumerTester(KeyValue properties, String queue, ArrayList<String> topics){
        this.properties = properties;
        this.queueName = queue;
        this.topics = topics;
        this.consumer = new DefaultPullConsumer(properties);
        this.consumer.attachQueue(queue,topics);
    }

    @Override
    public void run() {
        int count = 0;
        while(true){
            DefaultBytesMessage msg = (DefaultBytesMessage)consumer.poll();
//            System.out.println(msg);
            if (msg ==null){
                break;
            }
            String topic = msg.headers().getString(MessageHeader.TOPIC);
            String queue = msg.headers().getString(MessageHeader.QUEUE);
            String name = topic!= null ? topic: queue;
//            System.out.println("Header: "+ name + "Body: "+new String(msg.getBody()));
            count++;
        }
        System.out.println("Count:"+count);
    }
}
public class DemoTester {

    public static void main(String[] args) throws InterruptedException {
        KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
        properties.put("STORE_PATH", "/Users/KDF5000/Documents/2017/Coding/Middleware/data1");

        //构造测试数据
        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE1"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE2"; //实际测试时，queue数目与消费线程数目相同

        //生产
//        {
//            ArrayList<Topic> topics = new ArrayList<Topic>();
//            topics.add(new Topic(Constant.TYPE_TOPIC,"TOPIC1"));
//            topics.add(new Topic(Constant.TYPE_TOPIC,"TOPIC2"));
//            topics.add(new Topic(Constant.TYPE_QUEUE,"QUEUE1"));
//            topics.add(new Topic(Constant.TYPE_QUEUE,"QUEUE2"));
//
//            ExecutorService executorService = Executors.newFixedThreadPool(10);
//            long start = System.currentTimeMillis();
//            for(int i=0;i<10;i++){
//                executorService.execute(new ProducerTester(properties,topics));
//            }
//
//            Thread.sleep(2000);
//            executorService.shutdown();
//            long end = System.currentTimeMillis();
//            long T1 = end - start;
//            System.out.println("Producer Time:%s"+T1);
//        }

        //启动10个消费者去消费
        {
            ArrayList<String> topics1 = new ArrayList<String>();
            topics1.add(topic1);
            ArrayList<String> topics2 = new ArrayList<String>();
            topics2.add(topic1);topics2.add(topic2);

            ArrayList<String> topics3 = new ArrayList<String>();
            topics3.add(topic2);

            ExecutorService executorService2 = Executors.newFixedThreadPool(10);
            executorService2.execute(new ConsumerTester(properties, queue1, topics1));
            executorService2.execute(new ConsumerTester(properties, queue2, topics2));
            executorService2.execute(new ConsumerTester(properties, "NOQUEUE", topics3));

            executorService2.shutdown();
            executorService2.awaitTermination(10L, TimeUnit.SECONDS);
        }

    }
}
