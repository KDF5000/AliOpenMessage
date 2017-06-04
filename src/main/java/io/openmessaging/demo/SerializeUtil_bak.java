package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;

import java.io.*;

/**
 * Created by KDF5000 on 2017/5/29.
 */
public class SerializeUtil_bak {
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
        baos.write(SerializeUtil_bak.intToByte(type));
        baos.write(SerializeUtil_bak.intToByte(queueOrTopicLen));

        baos.write(queueOrTopic.getBytes());
        baos.write(SerializeUtil_bak.intToByte(bodyLen));
        baos.write(body);
        return baos.toByteArray();
    }
    //总长度 + KeyLen + Key + ValueLen + Value + ...
    public static byte[] serializeKeyValue(KeyValue kv){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteArrayOutputStream realData = new ByteArrayOutputStream();
        if (kv==null){
            try{
                baos.write(intToByte(0));
                return baos.toByteArray();
            }catch (IOException e){
                //
            }
            return null;
        }
        int totalLen = 0;
        for(String key:kv.keySet()){
            try{
                Object val = kv.getObject(key);
                ByteArrayOutputStream tmpByteArray = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(tmpByteArray);
                oos.writeObject(val);

                realData.write(intToByte(key.getBytes().length));
                realData.write(key.getBytes());

                byte[] tmpByte = tmpByteArray.toByteArray();
                realData.write(intToByte(tmpByte.length));
                realData.write(tmpByte);
//                System.out.println(tmpByte.length+","+realData.toByteArray().length);
            }catch (IOException e){

            }
        }
        try{
            byte []realBytes = realData.toByteArray();
            baos.write(realBytes);
//            System.out.println(realBytes.length + "," + baos.toByteArray().length);
            return baos.toByteArray();
        }catch (IOException e){
            //
        }

        return null;
    }

    //总长度 + KeyLen + Key + ValueLen + Value + ...
    public static void unserializeKeyValue(KeyValue kv, byte[] bytes){
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        //type
        int index = 0;
        while(index < bytes.length){
            try{
                byte []tmpBytes = new byte[4];
                //key
                bais.read(tmpBytes);
                int keyLen = SerializeUtil_bak.byteToInt(tmpBytes);
                byte []keyBytes = new byte[keyLen];
                bais.read(keyBytes);
                String key = new String(keyBytes);
//                System.out.println("Key:"+key);
                //val
                bais.read(tmpBytes);
                int valLen = SerializeUtil_bak.byteToInt(tmpBytes);
                byte []valBytes = new byte[valLen];
                bais.read(valBytes);
                ByteArrayInputStream tmpBais = new ByteArrayInputStream(valBytes);
                ObjectInputStream ois = new ObjectInputStream(tmpBais);
                Object valObj = ois.readObject();

//                System.out.println("Val:"+valObj.toString());
                kv.put(key, valObj);
                index += 4+keyLen+4+valLen;
            }catch (Exception e){

            }
        }

    }

    public static byte[]serializeMessageFull(Message msg){
        DefaultBytesMessage message = (DefaultBytesMessage)msg;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        //headers
        KeyValue headers = message.headers();
        byte[] headersBytes = serializeKeyValue(headers);
        KeyValue properies = message.properties();
        byte[] properiesBytes = serializeKeyValue(properies);
        byte[] body = message.getBody();

        try{
            baos.write(intToByte(headersBytes.length));
            baos.write(headersBytes);

            baos.write(intToByte(properiesBytes.length));
            baos.write(properiesBytes);

            baos.write(intToByte(body.length));
            baos.write(body);
        }catch (IOException e){
            //
        }
        return baos.toByteArray();
    }


    public static Object unserializeMessageFull(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        KeyValue headers = new DefaultKeyValue();
        KeyValue properies = new DefaultKeyValue();
        byte [] body = null;
        try{
            //headerLen
            byte []tmpBytes = new byte[4];
            bais.read(tmpBytes);
            int headerLen = SerializeUtil_bak.byteToInt(tmpBytes);
            if (headerLen>0){
                byte[] headersByte = new byte[headerLen];
                bais.read(headersByte);
                unserializeKeyValue(headers, headersByte);
//                System.out.println(headers.getString("Queue"));
            }
            //properies
            bais.read(tmpBytes);
            int properiesLen = SerializeUtil_bak.byteToInt(tmpBytes);
            if(properiesLen > 0){
                byte [] properiesByte = new byte[properiesLen];
                bais.read(properiesByte);
                unserializeKeyValue(properies, properiesByte);
//                System.out.println(properies.getObject("test"));
            }

            bais.read(tmpBytes);
            int bodyLen = SerializeUtil_bak.byteToInt(tmpBytes);
            body = new byte[bodyLen];
            bais.read(body);
        }catch (Exception e){
            //
        }

        DefaultBytesMessage msg = new DefaultBytesMessage(body);

        for (String key: headers.keySet()){
            Object obj = headers.getObject(key);
            msg.headers().put(key, obj);
        }
        for(String key: properies.keySet()){
            Object obj = properies.getObject(key);
            msg.properties().put(key, obj);
        }
        return msg;
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
        int type = SerializeUtil_bak.byteToInt(bytes);
        //TopicLen
        bais.read(tmpBytes, 0, 4);
        int topicLen = SerializeUtil_bak.byteToInt(tmpBytes);
        byte []topicBytes = new byte[topicLen];
        bais.read(topicBytes, 0, topicLen);
        String topic = new String(topicBytes);

        //body
        bais.read(tmpBytes, 0, 4);
        int bodyLen = SerializeUtil_bak.byteToInt(tmpBytes);
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
        msg.putProperties("test", "12");

//        byte []objs = SerializeUtil.serialize(msg);
        try{
            byte []objs = SerializeUtil_bak.serializeMessage(msg);
            byte []objs2 = SerializeUtil_bak.serializeMessageFull(msg);
            byte []objs3 = SerializeUtil_bak.serialize(msg);

            System.out.println(objs.length+","+objs2.length + ","+ objs3.length );

            DefaultBytesMessage msg2 = (DefaultBytesMessage) SerializeUtil_bak.unserializeMessageFull(objs2);
            System.out.println(msg2.headers().getString(MessageHeader.QUEUE));
            System.out.println(msg.properties().getString("test"));
            System.out.println(msg2.getBody().length);
//
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
