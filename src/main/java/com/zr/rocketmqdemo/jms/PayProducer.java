package com.zr.rocketmqdemo.jms;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Description:
 * 支付生产者
 *
 * @author zhangr
 * 2020/4/12 10:28
 */
@Component
public class PayProducer {

    private String producerGroup = "pay_group";

    private DefaultMQProducer producer;

    //指定NameServer地址，多个地址以 ; 隔开
    //如 producer.setNamesrvAddr("127.0.0.1:9876;127.0.0.2:9876;127.0.0.3:9876");
    public PayProducer(@Value("${customization.rocketMq.nameServerAddr}") String nameServerAddr) {
        producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(nameServerAddr);
        start();
    }

    public DefaultMQProducer getProducer() {
        return this.producer;
    }

    /**
     * 对象在使用之前必须要调用一次，只能初始化一次
     */
    public void start() {
        try {
            this.producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }


    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown() {
        this.producer.shutdown();
    }
}
