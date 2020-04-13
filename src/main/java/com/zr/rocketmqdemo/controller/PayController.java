package com.zr.rocketmqdemo.controller;

import com.zr.rocketmqdemo.jms.PayProducer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * Description:
 *
 * @author zhangr
 * 2020/4/12 14:35
 */
@Api
@RestController
@Slf4j
public class PayController {

    @Resource
    private PayProducer payProducer;

    @Value("${customization.rocketMq.payTopic}")
    private String topic;

    @GetMapping("/api/v1/sendTest")
    @ApiOperation(value = "sendTest")
    public Object sendTest(@RequestParam("name") String name) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        Message message = new Message(topic, "tagA", ("hello world, my name is " + name).getBytes());

        SendResult sendResult = payProducer.getProducer().send(message);

        log.info(sendResult.toString());

        return sendResult;
    }

}
