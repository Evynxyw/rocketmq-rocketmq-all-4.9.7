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
package org.apache.rocketmq.example.quickstart;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static final String CONSUMER_GROUP = "group_one";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTest";

    public static void main(String[] args) throws InterruptedException, MQClientException {

        // 创建一个默认的 Push 模式的消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        // 设置 NameServer 的地址
        consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        // 设置消费从头开始
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 订阅 TOPIC 中的所有消息，第二个参数为过滤匹配表达式，* 号表示需要订阅所有的消息
        consumer.subscribe(TOPIC, "*");

        // 扩展-官网参考：https://rocketmq.apache.org/zh/docs/4.x/consumer/02push#%E6%B6%88%E6%81%AF%E8%BF%87%E6%BB%A4
        // 表达式支持 Tag 过滤、SQL92 过滤
        // 举例：
        // 仅消费TagA标签的消息：consumer.subscribe("TagFilterTest", "TagA");
        // 消费A和B标签的消息：consumer.subscribe("TagFilterTest", "TagA||TagB");
        //
        // 通过 SQL 表达式消费：consumer.subscribe("SqlFilterTest", MessageSelector
        //        .bySql("(TAGS is not null and TAGS in ('TagA', 'TagB')) and (a is not null and a between 0 and 3)"));

        // 注册消息监听器，收到消息后进行处理
        consumer.registerMessageListener((MessageListenerConcurrently) (msg, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msg);

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 启动消费者
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
