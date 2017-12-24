package wxd.mq.model;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;

/**
 * Created by Winn on 2017/12/23.
 */
public class Consumer2 {

    public Consumer2(){

        try {

            String group_name = "message_producer";

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
            consumer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");
            consumer.subscribe("Topic1","Tag1 || Tage2 || Tage3");

            //广播模式下需要先启动Consumer（BROADCASTING广播模式与activemq发布订阅模式相同 不采用负载均衡 默认为负载均衡模式）
            //consumer.setMessageModel(MessageModel.BROADCASTING);

            consumer.registerMessageListener(new Listener());
            consumer.start();

        }catch (Exception e){
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        Consumer1 consumer2 = new Consumer1();
        System.out.println("Consumer2 start");
    }
}
