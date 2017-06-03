package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.demo.tester.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by KDF5000 on 2017/6/3.
 */
public class SingleFileConsume {

    public static void main(String []args){
//        String indexPath = "/Users/KDF5000/Documents/2017/Coding/Middleware/data1/1_QUEUE_0.index";
//        String dataPath = "/Users/KDF5000/Documents/2017/Coding/Middleware/data1/1_QUEUE_0.data";

        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", Constants.STORE_PATH);

        List<String> bucketList = new ArrayList<>();
        bucketList.add("QUEUE_0");
        ConsumeQueue consumeQueue = new ConsumeQueue("QUEUE_0", bucketList,properties);
        while(true){
            Message msg = consumeQueue.pollMessage();
            if(msg != null){
                System.out.println("Message: "+msg);
            }else{
                break;
            }
        }
    }
}
