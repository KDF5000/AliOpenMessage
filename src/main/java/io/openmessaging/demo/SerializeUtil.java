package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;

import java.io.*;

/**
 * Created by KDF5000 on 2017/5/29.
 */
public class SerializeUtil {
    public static byte[] serialize(Object object) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            //序列化
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Object unserialize(byte[] bytes) {
        ByteArrayInputStream bais = null;
        try {
            //反序列化
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
        }
        return null;
    }

    public static byte[]serializeMessage(Message msg) throws IOException{
        DefaultBytesMessage message = (DefaultBytesMessage)msg;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //只保存topic,queue和body
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        String queueOrTopic = topic!= null ? topic:queue;
        int queueOrTopicLen = queueOrTopic.length();

        int type = topic!= null ? Constant.TYPE_TOPIC:Constant.TYPE_QUEUE;

        byte []body = message.getBody();
        int bodyLen = body.length;

        //写缓冲: type+ topicLen + topic + bodyLen + body
//        int msgLen = 4 + queueOrTopicLen + 4 + bodyLen;
//        System.out.println(msgLen);
        baos.write(SerializeUtil.intToByte(type));
        baos.write(SerializeUtil.intToByte(queueOrTopicLen));

        baos.write(queueOrTopic.getBytes());
        baos.write(SerializeUtil.intToByte(bodyLen));
        baos.write(body);
        return baos.toByteArray();
    }

    public static int byteToInt(byte[] bytes){
        return bytes[3] & 0xff | (bytes[2] & 0xff) << 8 | (bytes[1] & 0xff) << 16 | (bytes[0] & 0xff) << 24;
    }

    public static byte[] intToByte(int num){
         return new byte[]{
                 (byte)((num >> 24) & 0xff),
                 (byte)((num >> 16) & 0xff),
                 (byte)((num >> 8) & 0xff),
                 (byte)((num) & 0xff)
         };
    }


    public static Message unserializeMessage(byte[] bytes){
        if (bytes == null || bytes.length == 0){
            return null;
        }
        //反序列化
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        //type
        byte []tmpBytes = new byte[4];
        bais.read(bytes, 0, 4);
        int type = SerializeUtil.byteToInt(bytes);
        //TopicLen
        bais.read(tmpBytes, 0, 4);
        int topicLen = SerializeUtil.byteToInt(tmpBytes);
        byte []topicBytes = new byte[topicLen];
        bais.read(topicBytes, 0, topicLen);
        String topic = new String(topicBytes);

        //body
        bais.read(tmpBytes, 0, 4);
        int bodyLen = SerializeUtil.byteToInt(tmpBytes);
        byte[] body = new byte[bodyLen];
        bais.read(body,0, bodyLen);

//        System.out.println(type+topic+bodyLen+new String(body));
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        if (type == Constant.TYPE_TOPIC){
            defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        }else{
            defaultBytesMessage.putHeaders(MessageHeader.QUEUE, topic);
        }
        return defaultBytesMessage;
    }

    public static void main(String[]args){
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/Users/KDF5000/Documents/2017/Coding/Middleware/data");

        Producer producer = new DefaultProducer(properties);
        Message msg = producer.createBytesMessageToQueue("QUEUE_1", "topic121".getBytes());

//        byte []objs = SerializeUtil.serialize(msg);
        try{
            byte []objs = SerializeUtil.serializeMessage(msg);

            DefaultBytesMessage msg2 = (DefaultBytesMessage)SerializeUtil.unserializeMessage(objs);
            System.out.println(objs.length);//329
            System.out.println(msg2.headers().getString(MessageHeader.QUEUE));
            System.out.println(msg2.getBody().length);
        }catch (Exception e){
            e.printStackTrace();
        }
//        DefaultBytesMessage msg2 = (DefaultBytesMessage)SerializeUtil.unserialize(objs);

    }
}
