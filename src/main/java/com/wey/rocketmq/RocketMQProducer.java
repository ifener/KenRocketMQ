package com.wey.rocketmq;

import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class RocketMQProducer {
    
    public static void main(String[] args) throws MQClientException {
        /** 
         * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br> 
         * 注意：ProducerGroupName需要由应用来保证唯一<br> 
         * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键， 
         * 因为服务器会回查这个Group下的任意一个Producer 
         */
        final DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        // nameserver服务,多个以;分开
        producer.setNamesrvAddr("192.168.72.100:9876");
        producer.setInstanceName("Producer");
        /** 
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可<br> 
         * 注意：切记不可以在每次发送消息时，都调用start方法 
         */
        producer.start();
        /** 
         * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag的消息。 
         * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态，<br> 
         * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高，<br> 
         * 需要对这种情况做处理。另外，消息可能会存在发送失败的情况，失败重试由应用来处理。 
         */
        
        for (int i = 0; i < 10; i++) {
            
            try {
                Message message = new Message("TopicA", "TagA", // tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
                                              "Order_A_" + System.currentTimeMillis(), // key
                                                                                       // 消息关键词，多个Key用KEY_SEPARATOR隔开（查询消息使用）
                                              ("Hello MetaQ A " + System.currentTimeMillis()).getBytes());// body
                SendResult sendResult = producer.send(message);
                System.out.println(sendResult);
                
                Message messageB = new Message("TopicB", "TagB", // tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
                                               "Order_B_" + System.currentTimeMillis(), // key
                                                                                        // 消息关键词，多个Key用KEY_SEPARATOR隔开（查询消息使用）
                                               ("Hello MetaQ B " + System.currentTimeMillis()).getBytes());// body
                SendResult sendResultB = producer.send(messageB);
                System.out.println(sendResultB);
                
                Message messageC = new Message("TopicA", "TagC", // tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
                                               "Order_C_" + System.currentTimeMillis(), // key
                                                                                        // 消息关键词，多个Key用KEY_SEPARATOR隔开（查询消息使用）
                                               ("Hello MetaQ C " + System.currentTimeMillis()).getBytes());// body
                SendResult sendResultC = producer.send(messageC);
                System.out.println(sendResultC);
                
                TimeUnit.MILLISECONDS.sleep(1000);
                
            }
            catch (RemotingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (MQBrokerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        }
        
        /** 
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己 
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法 
         */
        // producer.shutdown();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            
            public void run() {
                producer.shutdown();
            }
        }));
        System.exit(0);
    }
    
}
