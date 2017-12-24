package wxd.mq.model;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by Winn on 2017/12/23.
 */
public class Listener implements MessageListenerConcurrently {
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        try {
            for (MessageExt msg : msgs){
                String topic = msg.getTopic();
                String msgBody = new String(msg.getBody(),"UTF-8");
                String tags = msg.getTags();
                System.out.println("收到消息：" + " topic " + topic
                        + " ,tags: " + tags
                        + " ,msg：" + msgBody);
            }
        }catch (Exception e){
            e.printStackTrace();
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
