package com.zr.rocketmqdemo.jms;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Description:
 * 支付消费者
 *
 * @author zhangr
 * 2020/4/13 12:45
 */
@Component
@Slf4j
public class PayConsumer {


    private DefaultMQPushConsumer consumer;

    private String consumerGroup = "pay_consumer_group";

    public PayConsumer(@Value("${customization.rocketMq.nameServerAddr}") String nameServerAddr,
                       @Value("${customization.rocketMq.payTopic}") String payTopic) throws MQClientException {
        //实例化消费者
        consumer = new DefaultMQPushConsumer(consumerGroup);
        //设置nameServer
        consumer.setNamesrvAddr(nameServerAddr);
        //设置消费地址，自己选择ConsumeFromWhere策略，这个是默认在DefaultMQPushConsumer构造器设置好的
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //订阅消息，这里的subExpression参数就是tag
        consumer.subscribe(payTopic, "*");

        //lambda表达式
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                //单条进行消费
                Message msg = msgs.get(0);
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
                String topic = msg.getTopic();
                String body = new String(msg.getBody(), "utf-8");
                String tags = msg.getTags();
                String keys = msg.getKeys();
                System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (UnsupportedEncodingException e) {
                log.error("consumer error:", e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        consumer.start();
        log.info("---------consumer start--------");
    }

}
