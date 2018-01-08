package wxd.mq.filter;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by Winn on 2018/1/7.
 */
public class Consumer {

    public static void main(String[] args) throws Exception {

        String group_name = "filter_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
        consumer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");
        //使用java代码，在服务器做消息过滤
        String filterCode = MixAll.file2String("E:\\IdeaProject\\rocketmq\\rocketmqAPI\\src\\wxd\\mq\\filter\\MessageFilterImpl.java");
        System.out.println(filterCode);
        consumer.subscribe("TopicFilter7","wxd.mq.filter.MessageFilterImpl",filterCode);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + " Receive New Message: " + msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started");
    }
}
