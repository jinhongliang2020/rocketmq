package wxd.mq.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;


/**
 * Created by Winn on 2017/12/22.
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");

        producer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");

        producer.setRetryTimesWhenSendFailed(10);//失败的情况重发10次

        producer.start();
        try {
            for (int i=0; i<10; i++){
                Message msg = new Message("TopicQuickStart","TagA",(" Hello RocketMQ " + i).getBytes());
                SendResult sendResult = producer.send(msg,1000);//消息在1S内没有发送成功，就会重试
                System.out.println(sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Thread.sleep(1000);
        }

        producer.shutdown();


    }

}
