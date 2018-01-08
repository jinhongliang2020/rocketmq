package wxd.mq.filter;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * Created by Winn on 2018/1/6.
 */
public class Producer {

    public static void main(String[] args) throws Exception {

        String group_name = "filter_producer";
        DefaultMQProducer producer = new DefaultMQProducer(group_name);

        producer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");
        producer.start();

        for (int i = 0; i < 100; i++){
            Message message = new Message("TopicFilter7",
                    "TadA",
                    "OrderID001",
                    ("Hello RocketMQ " + i).getBytes());

            //给上传message添加一个字段规范
            message.putUserProperty("SequenceId",String.valueOf(i));

            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}
