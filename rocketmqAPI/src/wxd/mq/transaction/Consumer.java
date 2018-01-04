package wxd.mq.transaction;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_consumer");
        consumer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");
        consumer.subscribe("TopicTransaction","*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {


                MessageExt msg = msgs.get(0);
                try {

                    System.out.println("收到消息：" + " topic " + msg.getTopic()
                            + " ,tags: " + msg.getTags()
                            + " ,msg：" + new String(msg.getBody(),"utf-8"));

                }catch (Exception e){
                    e.printStackTrace();
                    //如果重试次数为2
                    if (msg.getReconsumeTimes() == 2) {
                        //记录日志 把异常消息记录后返回成功
                        System.out.println("记录失败消息" + msg.getTopic());
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;//如果失败告诉RocketMQ过会重新发送
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started");
    }
}
