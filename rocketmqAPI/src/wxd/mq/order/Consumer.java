package wxd.mq.order;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 *  顺序消息消费。带事物方式（应用可控制Offset什么时候提交）
 */
public class Consumer {

    public Consumer() throws Exception {

        String group_name = "order_producer";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
        consumer.setNamesrvAddr("192.168.0.121:9786;192.168.0.122:9786");

        /**
         *  设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
         *  如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //订阅的主题，以及过滤的标签内容
        consumer.subscribe("TopicTest", "order_1 || order_2 || order_3");
        //监听
        consumer.registerMessageListener(new Listener());
        consumer.start();
        System.out.println("Consumer Started.");
    }

    class Listener implements MessageListenerOrderly{

        private Random random = new Random();
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            //设置自动提交
            context.setAutoCommit(true);

            for (MessageExt msg:msgs){
                System.out.println(msg + ", content: " + new String(msg.getBody()));
            }

            try {
                TimeUnit.SECONDS.sleep(random.nextInt(5));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
    }
}


