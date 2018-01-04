package wxd.mq.order;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 *
 *  Producer，发送顺序消息
 *  rocketmq的顺序消息需要满足2点
 *  1. Producer端保证发送消息有序，且发送到同一个队列。
 *  2. Consumer端保证同一个队列消费
 */
public class Producer {

    public static void main(String[] args) {
        try{
            String group_name = "order_producer";

            DefaultMQProducer producer = new DefaultMQProducer(group_name);

            producer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");

            producer.start();

            String[] tags = new String[]{"TagA","TagB","TagC"};

            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            String dateStr = sdf.format(date);

            for (int i = 0; i < 5; i++){
                //时间戳
                String body = dateStr + " order_1: " + i;
                //参数： topic tag message
                Message msg = new Message("TopicTest","order_1", "KEY" + i, body.getBytes());
                //发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector，保证消息进入同一个队列中去。
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        return mqs.get(id);
                    }
                },0); //0是队列下标

                System.out.println(sendResult + ", body：" + body);
            }

            for (int i = 5; i < 10; i++){
                //时间戳
                String body = dateStr + " order_2: " + i;
                //参数： topic tag message
                Message msg = new Message("TopicTest","order_2", "KEY" + i, body.getBytes());
                //发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector，保证消息进入同一个队列中去。
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        return mqs.get(id);
                    }
                },1); //1是队列下标

                System.out.println(sendResult + ", body：" + body);
            }

            for (int i = 10; i < 15; i++){
                //时间戳
                String body = dateStr + " order_3: " + i;
                //参数： topic tag message
                Message msg = new Message("TopicTest","order_3", "KEY" + i, body.getBytes());
                //发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector，保证消息进入同一个队列中去。
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        return mqs.get(id);
                    }
                },2); //2是队列下标

                System.out.println(sendResult + ", body：" + body);
            }

            producer.shutdown();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        }
    }
}
