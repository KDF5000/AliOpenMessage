package io.messaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.demo.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[]args){
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/Users/KDF5000/Documents/2017/Coding/Middleware/data");

        Producer producer = new DefaultProducer(properties);
        Message msg = producer.createBytesMessageToTopic("topic1", "topic121".getBytes());

        byte []objs = SerializeUtil.serialize(msg);
        System.out.println(objs.length);//329
        DefaultBytesMessage msg2 = (DefaultBytesMessage)SerializeUtil.unserialize(objs);
        System.out.println(msg2.headers().getString(MessageHeader.TOPIC));
        System.out.println(msg2.getBody().length);
    }
}
