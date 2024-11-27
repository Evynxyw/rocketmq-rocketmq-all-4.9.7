/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 创建一个 MQ 的生产者，并且指定一个 MQ 的组名
        // 假设有一个生产者，两个消费者，那么如果生产者的组名为 ProducerGroupName，两个消费者的组名也是 ProducerGroupName，
        // 这种情况下，那么生产者发送一个消息的时候，两个消费者里面只有一个消费者能够接收到消息。
        // 如果两个消费者的组名不相同，那么这两个消费者都能接收到这一个消息。
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        // 指定生产者的 nameserver 地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 启动生产者
        producer.start();

        // 循环发送 128 次消息
        for (int i = 0; i < 128; i++)
            try {
                {
                    // 构建一个消息实体，通过构造函数指定 topic、tag、keys、body
                    // topic: 主题名称，集群内全局唯一。
                    // tag: 消息的过滤标签，消费者可通过 Tag 对消息进行过滤，仅接收指定标签的消息。比如说发送一个订单消息，可以通过 tag 来区分订单的类型是已支付还是未支付。
                    // keys: 消息的索引键，可通过设置不同的Key区分消息和快速查找消息。
                    // body: 消息体，消息的具体内容。
                    // 释义参考官方文档：https://rocketmq.apache.org/zh/docs/domainModel/04message
                    Message msg = new Message("TopicTest",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.shutdown();
    }
}
