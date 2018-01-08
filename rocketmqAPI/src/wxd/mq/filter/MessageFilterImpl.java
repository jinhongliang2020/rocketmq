package wxd.mq.filter;

import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Created by Winn on 2018/1/7.
 */
public class MessageFilterImpl implements MessageFilter {
    @Override
    public boolean match(MessageExt messageExt) {
        //getUserProperty
        String property = messageExt.getUserProperty("SequenceId");
        if (property != null){
            int id = Integer.parseInt(property);

            if ((id % 2) == 0){
                return true;
            }
        }
        return false;
    }
}
