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
    //0:String, 1:int, 2: long, 3: double
    public static byte getObjType(Object obj){
        if(obj instanceof String){
            return 0x0;
        }else if(obj instanceof Integer){
            return 0x01;
        }else if(obj instanceof Long){
            return 0x02;
        }else if(obj instanceof Double){
            return 0x03;
        }
        //unknown
        return 0x04;
    }

    //总长度 + KeyLen + Key + ValueLen + Value + ...
    public static byte[] serializeKeyValue(DefaultKeyValue kv){

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
//                ByteArrayOutputStream tmpByteArray = new ByteArrayOutputStream();
//                ObjectOutputStream oos = new ObjectOutputStream(tmpByteArray);
//                oos.writeObject(val);
                byte flag = getObjType(val);
                byte[] valBytes = val.toString().getBytes();

                realData.write(intToByte(key.getBytes().length));
                realData.write(key.getBytes());

                realData.write(flag);//
                realData.write(intToByte(valBytes.length));
                realData.write(valBytes);
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

    public static Object convertByte2Obj(byte[] bytes, byte flag){
        String str = new String(bytes);
        if(flag == 0x0){
            //string
            return str;
        }else if(flag == 0x01){
            //int
            return Integer.parseInt(str);
        }else if(flag == 0x02){
            return Long.parseLong(str);
        }else if(flag == 0x03){
            return Double.parseDouble(str);
        }
        return str;
    }

    //总长度 + KeyLen + Key + ValueLen + Value + ...
    public static void unserializeKeyValue(DefaultKeyValue kv, byte[] bytes){
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        //type
        int index = 0;
        while(index < bytes.length){
            try{
                byte []tmpBytes = new byte[4];
                //key
                bais.read(tmpBytes);
                int keyLen = SerializeUtil.byteToInt(tmpBytes);
                byte []keyBytes = new byte[keyLen];
                bais.read(keyBytes);
                String key = new String(keyBytes);
//                System.out.println("Key:"+key);
                //val
                byte []flag = new byte[1];
                bais.read(flag);

                bais.read(tmpBytes);
                int valLen = SerializeUtil.byteToInt(tmpBytes);
                byte []valBytes = new byte[valLen];
                bais.read(valBytes);
                Object obj = convertByte2Obj(valBytes, flag[0]);
//                ByteArrayInputStream tmpBais = new ByteArrayInputStream(valBytes);
//                ObjectInputStream ois = new ObjectInputStream(tmpBais);
//                Object valObj = ois.readObject();

//                System.out.println("Val:"+valObj.toString());
                kv.put(key, obj);
                index += 4+keyLen+1+4+valLen;
            }catch (Exception e){

            }
        }

    }

    public static byte[]serializeMessageFull(Message msg){
        DefaultBytesMessage message = (DefaultBytesMessage)msg;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        //headers
        KeyValue headers = message.headers();
        byte[] headersBytes = serializeKeyValue((DefaultKeyValue) headers);
        KeyValue properies = message.properties();
        byte[] properiesBytes = serializeKeyValue((DefaultKeyValue) properies);
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
            int headerLen = SerializeUtil.byteToInt(tmpBytes);
            if (headerLen>0){
                byte[] headersByte = new byte[headerLen];
                bais.read(headersByte);
                unserializeKeyValue((DefaultKeyValue) headers, headersByte);
//                System.out.println(headers.getString("Queue"));
            }
            //properies
            bais.read(tmpBytes);
            int properiesLen = SerializeUtil.byteToInt(tmpBytes);
            if(properiesLen > 0){
                byte [] properiesByte = new byte[properiesLen];
                bais.read(properiesByte);
                unserializeKeyValue((DefaultKeyValue) properies, properiesByte);
//                System.out.println(properies.getObject("test"));
            }

            bais.read(tmpBytes);
            int bodyLen = SerializeUtil.byteToInt(tmpBytes);
            body = new byte[bodyLen];
            bais.read(body);
        }catch (Exception e){
            //
        }

        DefaultBytesMessage msg = new DefaultBytesMessage(body);

        for (String key: headers.keySet()){
            Object obj = ((DefaultKeyValue)headers).getObject(key);
            ((DefaultKeyValue)msg.headers()).put(key, obj);
        }
        for(String key: properies.keySet()){
            Object obj = ((DefaultKeyValue)properies).getObject(key);
            ((DefaultKeyValue)msg.properties()).put(key, obj);
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
        msg.putProperties("test", "12");
        msg.putProperties("test1", 12131313);
        msg.putProperties("test2", 12.3);

//        byte []objs = SerializeUtil.serialize(msg);
        try{
            byte []objs = SerializeUtil.serializeMessage(msg);
            System.out.println(System.currentTimeMillis());
            byte []objs2 = SerializeUtil.serializeMessageFull(msg);
            System.out.println(System.currentTimeMillis());
            byte []objs3 = SerializeUtil.serialize(msg);

            System.out.println(objs.length+","+objs2.length + ","+ objs3.length );
            System.out.println(System.currentTimeMillis());
            DefaultBytesMessage msg2 = (DefaultBytesMessage)SerializeUtil.unserializeMessageFull(objs2);
            System.out.println(System.currentTimeMillis());
            System.out.println(msg2.headers().getString(MessageHeader.QUEUE));
            System.out.println(msg.properties().getString("test"));
            System.out.println(msg.properties().getInt("test1"));
            System.out.println(msg.properties().getDouble("test2"));
            System.out.println(msg2.getBody().length);
//
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
