/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.File;
import java.io.IOException;

import javax.management.JMException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * This class starts and runs a standalone ZooKeeperServer.
 */
public class ZooKeeperServerMain {
    private static final Logger LOG =
        LoggerFactory.getLogger(ZooKeeperServerMain.class);

    private static final String USAGE =
        "Usage: ZooKeeperServerMain configfile | port datadir [ticktime] [maxcnxns]";

    private ServerCnxnFactory cnxnFactory;

    /*
     * Start up the ZooKeeper server.
     *
     * @param args the configfile or the port datadir [ticktime]
     */
    public static void main(String[] args) {
        ZooKeeperServerMain main = new ZooKeeperServerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    protected void initializeAndRun(String[] args)
        throws ConfigException, IOException
    {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        ServerConfig config = new ServerConfig();
        //解析配置文件zoo.cfg
        if (args.length == 1) {
            config.parse(args[0]);
        } else {
            config.parse(args);
        }
        //启动服务端
        runFromConfig(config);
    }

    /**
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException
     */
    public void runFromConfig(ServerConfig config) throws IOException {
        LOG.info("Starting server");
        try {
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            ZooKeeperServer zkServer = new ZooKeeperServer();
            //log和data文件
            FileTxnSnapLog ftxn = new FileTxnSnapLog(new
                   File(config.dataLogDir), new File(config.dataDir));
            zkServer.setTxnLogFactory(ftxn);
            zkServer.setTickTime(config.tickTime);//设置ZooKeeper服务端的tickTime默认为3000(毫秒) 并作为SessionTrackerImpl中的ExpirationInterval过期检查时间间隔 对应nextExpiration=((currentTime+timeout)/ExpirationInterval + 1) * ExpirationInterval
            zkServer.setMinSessionTimeout(config.minSessionTimeout);
            zkServer.setMaxSessionTimeout(config.maxSessionTimeout);
            //构造连接工程   默认为NIOServerCnxnFactory    3.4.0版本后提供了Netty实现 可通过配置系统属性zookeeper.serverCnxnFactory来指定使用哪种方式
            cnxnFactory = ServerCnxnFactory.createFactory();
            //初始化主线程 打开selector并bind端口 打开NIO的OP_ACCEPT通知
            cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());
            //生成最新的snapshot文件,启动IO主线程  从snapshot文件和log文件中恢复内存database结构和session结构
            cnxnFactory.startup(zkServer);
            cnxnFactory.join();//等待之前启动的线程结束
            if (zkServer.isRunning()) {
                zkServer.shutdown();
            }
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        }
    }

    /**
     * Shutdown the serving instance
     */
    protected void shutdown() {
        cnxnFactory.shutdown();
    }
}
