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

package org.apache.zookeeper.server.quorum;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;

public class QuorumPeerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerConfig.class);

    protected InetSocketAddress clientPortAddress;
    protected String dataDir;
    protected String dataLogDir;
    protected int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    protected int maxClientCnxns = 60;
    /** defaults to -1 if not set explicitly */
    protected int minSessionTimeout = -1;
    /** defaults to -1 if not set explicitly */
    protected int maxSessionTimeout = -1;

    protected int initLimit;
    protected int syncLimit;
    protected int electionAlg = 3;
    protected int electionPort = 2182;
    protected final HashMap<Long,QuorumServer> servers =
        new HashMap<Long, QuorumServer>();
    protected final HashMap<Long,QuorumServer> observers =
        new HashMap<Long, QuorumServer>();

    protected long serverId;
    protected HashMap<Long, Long> serverWeight = new HashMap<Long, Long>();
    protected HashMap<Long, Long> serverGroup = new HashMap<Long, Long>();
    protected int numGroups = 0;
    protected QuorumVerifier quorumVerifier;
    protected int snapRetainCount = 3;
    protected int purgeInterval = 0;

    protected LearnerType peerType = LearnerType.PARTICIPANT;
    
    /**
     * Minimum snapshot retain count.
     * @see org.apache.zookeeper.server.PurgeTxnLog#purge(File, File, int)
     */
    private final int MIN_SNAP_RETAIN_COUNT = 3;

    @SuppressWarnings("serial")
    public static class ConfigException extends Exception {
        public ConfigException(String msg) {
            super(msg);
        }
        public ConfigException(String msg, Exception e) {
            super(msg, e);
        }
    }

    /**
     * Parse a ZooKeeper configuration file
     * @param path the patch of the configuration file
     * @throws ConfigException error processing configuration
     */
    public void parse(String path) throws ConfigException {
        File configFile = new File(path);

        LOG.info("Reading configuration from: " + configFile);

        try {
            if (!configFile.exists()) {
                throw new IllegalArgumentException(configFile.toString()
                        + " file is missing");
            }

            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }
            //解析转化成properties的zoo.cfg配置文件
            parseProperties(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + path, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + path, e);
        }
    }

    /**
     * Parse config from a Properties.
     * @param zkProp Properties to parse from.
     * @throws IOException
     * @throws ConfigException
     */
    public void parseProperties(Properties zkProp)
    throws IOException, ConfigException {
        int clientPort = 0;
        String clientPortAddress = null;
        for (Entry<Object, Object> entry : zkProp.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.equals("dataDir")) {//数据文件保存路径  用于存储内存数据快照的文件路径 同时用于集群的myid文件也位于该路径下
                dataDir = value;
            } else if (key.equals("dataLogDir")) {//日志文件保存路径
                dataLogDir = value;
            } else if (key.equals("clientPort")) {//服务端监听端口
                clientPort = Integer.parseInt(value);
            } else if (key.equals("clientPortAddress")) {//
                clientPortAddress = value.trim();
            } else if (key.equals("tickTime")) {//心跳时间(单位:毫秒)默认3000毫秒即3秒  为了确保连接存在,最新超时时间为2个心跳时间
                tickTime = Integer.parseInt(value);
            } else if (key.equals("maxClientCnxns")) {//最大并发客户端数,用于防止DOS 默认是10 若设置为0 表示不加限制
                maxClientCnxns = Integer.parseInt(value);
            } else if (key.equals("minSessionTimeout")) {//最小客户端session超时时间, 默认是2个ticktime 单位:毫秒
                minSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("maxSessionTimeout")) {//最大客户端session超时时间,默认是20个ticktime 单位:毫秒
                maxSessionTimeout = Integer.parseInt(value);
            } else if (key.equals("initLimit")) {//集群参数---多少个ticktime内允许其他server连接并初始化数据，如果zookeeper管理的数据较大,则相应增大这个值
                initLimit = Integer.parseInt(value);
            } else if (key.equals("syncLimit")) {//集群参数---多少个ticktime内允许follower同步,如果follower落后太多则会被丢弃
                syncLimit = Integer.parseInt(value);
            } else if (key.equals("electionAlg")) {//集群参数---用于选举实现的参数 0:表示以原始的基于UDP的方式协作,1:表示不进行用户验证的基于UDP的快速选举 2:表示进行用户验证的基于UDP的快速选举 3:表示基于TCP的快速选举 默认为3
                electionAlg = Integer.parseInt(value);
            } else if (key.equals("peerType")) {//节点类型  观察者 还是 follower
                if (value.toLowerCase().equals("observer")) {
                    peerType = LearnerType.OBSERVER;
                } else if (value.toLowerCase().equals("participant")) {
                    peerType = LearnerType.PARTICIPANT;
                } else
                {
                    throw new ConfigException("Unrecognised peertype: " + value);
                }
            } else if (key.equals("autopurge.snapRetainCount")) {//指定zookeeper的log和snapshot清理频率 单位是小时 需要填写一个1或者更多的整数,默认为0 表示不启动自己清理功能
                snapRetainCount = Integer.parseInt(value);
            } else if (key.equals("autopurge.purgeInterval")) {//次参数与autopurge.snapRetainCount配合使用,表示清理后保留文件的数目 默认是3个
                purgeInterval = Integer.parseInt(value);
            } else if (key.startsWith("server.")) {// 集群参数---配置集群里的主机信息  server.[myid]=[主机IP/hostname]:[同步端口]:[选举端口]:observer
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));//dataDir路径下myid中的值
                String parts[] = value.split(":");
                if ((parts.length != 2) && (parts.length != 3) && (parts.length !=4)) {
                    LOG.error(value
                       + " does not have the form host:port or host:port:port " +
                       " or host:port:port:type");
                }
                InetSocketAddress addr = new InetSocketAddress(parts[0],
                        Integer.parseInt(parts[1]));//主机IP/hostname + 与leader通信的端口
                if (parts.length == 2) {//长度为2的情况 server.[myid]=[主机IP/hostname]:[同步端口]
                    servers.put(Long.valueOf(sid), new QuorumServer(sid, addr));
                } else if (parts.length == 3) {//长度为3的情况server.[myid]=[主机IP/hostname]:[同步端口]:[选举端口]
                    InetSocketAddress electionAddr = new InetSocketAddress(
                            parts[0], Integer.parseInt(parts[2]));//主机IP/hostname + 选举端口
                    servers.put(Long.valueOf(sid), new QuorumServer(sid, addr,
                            electionAddr));
                } else if (parts.length == 4) {//长度为4 的情况server.[myid]=[主机IP/hostname]:[同步端口]:[选举端口]:observer
                    InetSocketAddress electionAddr = new InetSocketAddress(
                            parts[0], Integer.parseInt(parts[2]));
                    LearnerType type = LearnerType.PARTICIPANT;//节点类型默认值为PARTICIPANT
                    if (parts[3].toLowerCase().equals("observer")) {//节点类型为 observer
                        type = LearnerType.OBSERVER;
                        observers.put(Long.valueOf(sid), new QuorumServer(sid, addr,
                                electionAddr,type));
                    } else if (parts[3].toLowerCase().equals("participant")) {//节点类型为participant
                        type = LearnerType.PARTICIPANT;
                        servers.put(Long.valueOf(sid), new QuorumServer(sid, addr,
                                electionAddr,type));
                    } else {
                        throw new ConfigException("Unrecognised peertype: " + value);
                    }
                }
            } else if (key.startsWith("group")) {//集群参数---机器分组 例如：group.1=1:2:3 group.2=4:5:6  weight.1=1 weight.2=1 weight.3=1 weight.4=1 weight.5=1 weight.6=1
                int dot = key.indexOf('.');
                long gid = Long.parseLong(key.substring(dot + 1));

                numGroups++;

                String parts[] = value.split(":");
                for(String s : parts){
                    long sid = Long.parseLong(s);
                    if(serverGroup.containsKey(sid))
                        throw new ConfigException("Server " + sid + "is in multiple groups");
                    else
                        serverGroup.put(sid, gid);
                }

            } else if(key.startsWith("weight")) {//集群参数---机器权重
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                serverWeight.put(sid, Long.parseLong(value));
            } else {//设置环境变量
                System.setProperty("zookeeper." + key, value);
            }
        }
        
        // Reset to MIN_SNAP_RETAIN_COUNT if invalid (less than 3)
        // PurgeTxnLog.purge(File, File, int) will not allow to purge less
        // than 3.
        if (snapRetainCount < MIN_SNAP_RETAIN_COUNT) {
            LOG.warn("Invalid autopurge.snapRetainCount: " + snapRetainCount
                    + ". Defaulting to " + MIN_SNAP_RETAIN_COUNT);
            snapRetainCount = MIN_SNAP_RETAIN_COUNT;
        }

        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir is not set");
        }
        if (dataLogDir == null) {
            dataLogDir = dataDir;
        } else {
            if (!new File(dataLogDir).isDirectory()) {
                throw new IllegalArgumentException("dataLogDir " + dataLogDir
                        + " is missing.");
            }
        }
        if (clientPort == 0) {
            throw new IllegalArgumentException("clientPort is not set");
        }
        if (clientPortAddress != null) {
            this.clientPortAddress = new InetSocketAddress(
                    InetAddress.getByName(clientPortAddress), clientPort);
        } else {
            this.clientPortAddress = new InetSocketAddress(clientPort);
        }

        if (tickTime == 0) {
            throw new IllegalArgumentException("tickTime is not set");
        }
        if (minSessionTimeout > maxSessionTimeout) {
            throw new IllegalArgumentException(
                    "minSessionTimeout must not be larger than maxSessionTimeout");
        }
        if (servers.size() == 0) {
            if (observers.size() > 0) {
                throw new IllegalArgumentException("Observers w/o participants is an invalid configuration");
            }
            // Not a quorum configuration so return immediately - not an error
            // case (for b/w compatibility), server will default to standalone
            // mode.
            return;
        } else if (servers.size() == 1) {
            if (observers.size() > 0) {
                throw new IllegalArgumentException("Observers w/o quorum is an invalid configuration");
            }

            // HBase currently adds a single server line to the config, for
            // b/w compatibility reasons we need to keep this here.
            LOG.error("Invalid configuration, only one server specified (ignoring)");
            servers.clear();
        } else if (servers.size() > 1) {
            if (servers.size() == 2) {//至少3台机器组成集群
                LOG.warn("No server failure will be tolerated. " +
                    "You need at least 3 servers.");
            } else if (servers.size() % 2 == 0) {//集群必须是奇数台机器
                LOG.warn("Non-optimial configuration, consider an odd number of servers.");
            }
            if (initLimit == 0) {
                throw new IllegalArgumentException("initLimit is not set");
            }
            if (syncLimit == 0) {
                throw new IllegalArgumentException("syncLimit is not set");
            }
            /*
             * If using FLE, then every server requires a separate election
             * port.如果是用快速leader选举 每台服务器要求有一个独立的选举端口 即每台服务器需要有一个electionAddr
             */
            if (electionAlg != 0) {
                for (QuorumServer s : servers.values()) {
                    if (s.electionAddr == null)
                        throw new IllegalArgumentException(
                                "Missing election port for server: " + s.id);
                }
            }

            /*
             * Default of quorum config is majority
             */
            if(serverGroup.size() > 0){
                if(servers.size() != serverGroup.size())//确保每台服务器都被分组
                    throw new ConfigException("Every server must be in exactly one group");
                /*
                 * The deafult weight of a server is 1  默认服务器的权重都是1
                 */
                for(QuorumServer s : servers.values()){
                    if(!serverWeight.containsKey(s.id))
                        serverWeight.put(s.id, (long) 1);
                }

                /*
                 * 设置quorumVerifier为QuorumHierarchical
                 * Set the quorumVerifier to be QuorumHierarchical
                 */
                quorumVerifier = new QuorumHierarchical(numGroups,
                        serverWeight, serverGroup);
            } else {
                /*
                 * The default QuorumVerifier is QuorumMaj
                 */
            	//默认QuorumVerifier为QuorumMaj
                LOG.info("Defaulting to majority quorums");
                quorumVerifier = new QuorumMaj(servers.size());
            }

            // Now add observers to servers, once the quorums have been
            // figured out
            servers.putAll(observers);
          //读取dataDir路径下的myid文件 确定当前服务器的sid值 即serverId
            File myIdFile = new File(dataDir, "myid");
            if (!myIdFile.exists()) {
                throw new IllegalArgumentException(myIdFile.toString()
                        + " file is missing");
            }
            BufferedReader br = new BufferedReader(new FileReader(myIdFile));
            String myIdString;
            try {
                myIdString = br.readLine();
            } finally {
                br.close();
            }
            try {
                serverId = Long.parseLong(myIdString);
                MDC.put("myid", myIdString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("serverid " + myIdString
                        + " is not a number");
            }
            
            // Warn about inconsistent peer type
            LearnerType roleByServersList = observers.containsKey(serverId) ? LearnerType.OBSERVER
                    : LearnerType.PARTICIPANT;
            if (roleByServersList != peerType) {
                LOG.warn("Peer type from servers list (" + roleByServersList
                        + ") doesn't match peerType (" + peerType
                        + "). Defaulting to servers list.");
    
                peerType = roleByServersList;
            }
        }
    }

    public InetSocketAddress getClientPortAddress() { return clientPortAddress; }
    public String getDataDir() { return dataDir; }
    public String getDataLogDir() { return dataLogDir; }
    public int getTickTime() { return tickTime; }
    public int getMaxClientCnxns() { return maxClientCnxns; }
    public int getMinSessionTimeout() { return minSessionTimeout; }
    public int getMaxSessionTimeout() { return maxSessionTimeout; }

    public int getInitLimit() { return initLimit; }
    public int getSyncLimit() { return syncLimit; }
    public int getElectionAlg() { return electionAlg; }
    public int getElectionPort() { return electionPort; }    
    
    public int getSnapRetainCount() {
        return snapRetainCount;
    }

    public int getPurgeInterval() {
        return purgeInterval;
    }

    public QuorumVerifier getQuorumVerifier() {   
        return quorumVerifier;
    }

    public Map<Long,QuorumServer> getServers() {
        return Collections.unmodifiableMap(servers);
    }

    public long getServerId() { return serverId; }

    public boolean isDistributed() { return servers.size() > 1; }

    public LearnerType getPeerType() {
        return peerType;
    }
}
