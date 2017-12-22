package wxd.mq.quickstart;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by Winn on 2017/12/22.
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
        consumer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");
        /**
         *  设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
         *  如果非第一次启动，那么按照上次消费位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //最多一次拉取10条消息（但是在先启动Consumer情况下它还是会一条一条获取消息） 默认为一条
        consumer.setConsumeMessageBatchMaxSize(10);
        consumer.subscribe("TopicQuickStart","*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                System.out.println("消息条数：" + msgs.size());
                MessageExt msg = msgs.get(0);
                try {

                    System.out.println("收到消息：" + " topic " + msg.getTopic()
                            + " ,tags: " + msg.getTags()
                            + " ,msg：" + new String(msg.getBody(),"utf-8"));

                    //一定先启动consumer，在进行发送消息（也就是先订阅，再发送）
                    if (" Hello RocketMQ 4".equals(new String(msg.getBody(),"utf-8"))){
                        System.out.println("========失败消息开始========");
                        System.out.println(msg);
                        System.out.println(new String(msg.getBody(),"utf-8"));
                        System.out.println("========失败消息结束========");

                        //异常
                        int a = 1/0;
                    }
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
