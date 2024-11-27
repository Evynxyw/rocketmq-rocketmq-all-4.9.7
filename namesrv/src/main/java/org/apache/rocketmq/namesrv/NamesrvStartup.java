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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

public class NamesrvStartup {

    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    /**
     * NameServer 服务运行的入口
     */
    public static void main(String[] args) {
        // 调用 main0 这个真正的主方法
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        try {
            // 创建一个 NameServer 的控制器来处理所有的请求（比如说 broker 需要注册到 NameServer 里，或者生产者需要从 NameServer 里获取元数据）
            NamesrvController controller = createNamesrvController(args);
            // 控制器，启动！！
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        // 设置 MQ 的版本号到 rocketmq.remoting.version 变量中
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        // 解析启动时的命令行参数，比如说执行命令 `./mqnamesrv h` 的时候，h 就是命令行参数，执行后需要输出帮助信息
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        // 如果解析失败则直接退出进程
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        // 创建 NameServer 的配置参数实体
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        // 创建 Netty 服务器的配置参数实体，RocketMQ 默认使用 Netty 作为网络传输框架
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        // RocketMQ 默认使用 9876 作为通信端口
        nettyServerConfig.setListenPort(9876);

        // 执行命令若是 `./mqnamesrv -c xx.properties` 的话，那么此处需要将 xx.properties 加载进来
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 通过输入流的方式读取配置文件，并通过 Properties 进行加载
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);

                // 再接着将其配置写入到 namesrvConfig 和 nettyServerConfig 中
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                // 设置 nameServer 的配置文件地址
                namesrvConfig.setConfigStorePath(file);

                // 直接打印日志，表示加载配置文件完毕！
                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        // 执行命令若是 `./mqnamesrv -p` 的话，那么就需要打印出 namesrvConfig 和 nettyServerConfig 中的所有配置并退出进程
        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }

        // 将执行命令时的所有命令行参数传入到 namesrvConfig 中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        // 如果 ROCKET_HOME 没有配置，那么直接输出异常日志并且退出进程
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // 获取 Logback 的上下文对象 LoggerContext
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        // 获取 Logback 的配置器 JoranConfigurator
        JoranConfigurator configurator = new JoranConfigurator();
        // 将 LoggerContext 设置给 JoranConfigurator
        configurator.setContext(lc);
        // 重置，清除之前可能已经存在的配置
        lc.reset();
        // 加载并且应用这个配置文件，完成日志系统的配置
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        // 打印 namesrvConfig 和 nettyServerConfig 的配置信息
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        // 创建 NamesrvController 控制器，将 namesrvConfig 和 nettyServerConfig 配置传递进去
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        // 将外部文件的 properties 也合并到控制器里面的配置对象中
        controller.getConfiguration().registerConfig(properties);

        // 返回控制器，以便后续使用
        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {
        // 控制器如果为空，则抛出异常，终止进程
        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        // 执行控制器的初始化操作
        boolean initResult = controller.initialize();
        // 如果初始化失败，则直接退出进程
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        // 添加一个 JVM 进程关闭的钩子函数，保证在关闭前调用 controller 的 shutdown 方法
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        // 控制器，启动！！
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
