package wxd.mq.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

public class Producer {

    public static void main(String[] args) throws Exception {

        String group_name = "transaction_producer";

        final TransactionMQProducer producer = new TransactionMQProducer(group_name);

        //nameserver服务
        producer.setNamesrvAddr("192.168.0.121:9876;192.168.0.122:9876");
        //事务回查最小并发数
        producer.setCheckThreadPoolMinSize(5);
        //事务回查最大并发数
        producer.setCheckThreadPoolMaxSize(20);
        //队列数
        producer.setCheckRequestHoldMax(2000);

        producer.start();

        //服务器回调Producer，检查本地事务分支成功还是失败(这个功能在开源版本被去掉了)
        producer.setTransactionCheckListener(new TransactionCheckListener() {
            @Override
            public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
                //处理业务逻辑 判断返回结果
                System.out.println("state -- " + new String(msg.getBody()));
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        TransactionExecuterImpl transactionExecuter = new TransactionExecuterImpl();

        for (int i=1; i<=2; i++){

            Message msg = new Message("TopicTransaction",
                    "Transaction" + i,
                    "key",
                    ("Hello Rocket " + i).getBytes());
            SendResult sendResult = producer.sendMessageInTransaction(msg,transactionExecuter,"tq");
            System.out.println(sendResult);

            TimeUnit.MILLISECONDS.sleep(1000);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                producer.shutdown();
            }
        }));
        System.exit(0);
    }
}
