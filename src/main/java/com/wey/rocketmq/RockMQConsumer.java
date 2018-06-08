package com.wey.rocketmq;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class RockMQConsumer {
    
    /** 
     * 当前例子是Consumer用法，使用方式给用户感觉是消息从RocketMQ服务器推到了应用客户端。<br> 
     * 但是实际Consumer内部是使用长轮询Pull方式从MetaQ服务器拉消息，然后再回调用户Listener方法<br> 
     *  
     * @throws MQClientException 
     */
    public static void main(String[] args) throws MQClientException {
        /** 
         * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br> 
         * 注意：ConsumerGroupName需要由应用来保证唯一 ,最好使用服务的包名区分同一服务,一类Consumer集合的名称，这类Consumer通常消费一类消息，且消费逻辑一致 
         * PushConsumer：Consumer的一种，应用通常向Consumer注册一个Listener监听器，Consumer收到消息立刻回调Listener监听器 
         * PullConsumer：Consumer的一种，应用通常主动调用Consumer的拉取消息方法从Broker拉消息，主动权由应用控制 
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupName");
        consumer.setNamesrvAddr("192.168.72.100:9876");
        consumer.setInstanceName("Consumer");
        
        // 设置批量消费个数,默认是1，即一次消费一条参数
        // consumer.setConsumeMessageBatchMaxSize(10);
        
        /** 
         * 订阅指定topic下tags分别等于TagA或TagC或TagD 
         */
        consumer.subscribe("TopicA", "TagA || TagC || TagD");
        /** 
         * 订阅指定topic下所有消息<br> 
         * 注意：一个consumer对象可以订阅多个topic 
         *  
         */
        consumer.subscribe("TopicB", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(Thread.currentThread().getName() + " Receive New Messages " + msgs.size());
                MessageExt msg = msgs.get(0);
                if (msg.getTopic().equals("TopicA")) {
                    // 执行TopicTest1的消费逻辑
                    if (msg.getTags() != null && msg.getTags().equals("TagA")) {
                        // 执行TagA的消费
                        System.out.println(msg.getKeys() + "--" + new String(msg.getBody()));
                    }
                    else if (msg.getTags() != null && msg.getTags().equals("TagC")) {
                        // 执行TagC的消费
                        System.out.println(msg.getKeys() + "--" + new String(msg.getBody()));
                    }
                    else if (msg.getTags() != null && msg.getTags().equals("TagD")) {
                        // 执行TagD的消费
                        System.out.println(msg.getKeys() + "--" + new String(msg.getBody()));
                    }
                    
                }
                else if (msg.getTopic().equals("TopicB")) {
                    System.out.println(msg.getKeys() + "--" + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("ConsumerStarted.");
    }
    
}
