package wxd.mq.model;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * Created by Winn on 2017/12/23.
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        String group_name = "message_producer";

        DefaultMQProducer producer = new DefaultMQProducer(group_name);

        producer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");

        producer.start();

        for (int i=0; i<100; i++){
            try {
                Message msg = new Message("Topic1","Tag1",(" 信息内容 " + i).getBytes());
                SendResult sendResult = producer.send(msg,2000);//消息在1S内没有发送成功，就会重试
                System.out.println(sendResult);
            }catch (Exception e){
                e.printStackTrace();
                Thread.sleep(1000);
            }

        }
    }
}
