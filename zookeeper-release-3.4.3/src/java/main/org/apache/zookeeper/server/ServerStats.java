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


/**
 * ZooKeeper服务器运行时统计器
 * Basic Server Statistics
 */
public class ServerStats {
    private long packetsSent;//从ZooKeeper启动开始,或是最近一次重置服务端统计信息以后,服务端向客户端发送响应包次数
    private long packetsReceived;//从ZooKeeper启动开始,或是最近一次重置服务端统计信息以后,服务端接收的来自客户端的请求包次数
    private long maxLatency;//从ZooKeeper启动开始,或是最近一次重置服务端统计信息以后,服务端请求处理的最大延时
    private long minLatency = Long.MAX_VALUE;//从ZooKeeper启动开始,或是最近一次重置服务端统计信息以后,服务端请求处理的最小延时
    private long totalLatency = 0;//从ZooKeeper启动开始,或是最近一次重置服务端统计信息以后,服务端请求处理的总延时
    private long count = 0;//从ZooKeeper启动开始,或是最近一次重置服务端统计信息以后,服务端处理客户端请求的总次数

    private final Provider provider;

    public interface Provider {
        public long getOutstandingRequests();
        public long getLastProcessedZxid();
        public String getState();
    }
    
    public ServerStats(Provider provider) {
        this.provider = provider;
    }
    
    // getters
    synchronized public long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }

    synchronized public long getAvgLatency() {
        if (count != 0) {
            return totalLatency / count;
        }
        return 0;
    }

    synchronized public long getMaxLatency() {
        return maxLatency;
    }

    public long getOutstandingRequests() {
        return provider.getOutstandingRequests();
    }
    
    public long getLastProcessedZxid(){
        return provider.getLastProcessedZxid();
    }
    
    synchronized public long getPacketsReceived() {
        return packetsReceived;
    }

    synchronized public long getPacketsSent() {
        return packetsSent;
    }

    public String getServerState() {
        return provider.getState();
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Latency min/avg/max: " + getMinLatency() + "/"
                + getAvgLatency() + "/" + getMaxLatency() + "\n");
        sb.append("Received: " + getPacketsReceived() + "\n");
        sb.append("Sent: " + getPacketsSent() + "\n");
        if (provider != null) {
            sb.append("Outstanding: " + getOutstandingRequests() + "\n");
            sb.append("Zxid: 0x"+ Long.toHexString(getLastProcessedZxid())+ "\n");
        }
        sb.append("Mode: " + getServerState() + "\n");
        return sb.toString();
    }
    // mutators
    synchronized void updateLatency(long requestCreateTime) {
        long latency = System.currentTimeMillis() - requestCreateTime;
        totalLatency += latency;
        count++;
        if (latency < minLatency) {
            minLatency = latency;
        }
        if (latency > maxLatency) {
            maxLatency = latency;
        }
    }
    synchronized public void resetLatency(){
        totalLatency = 0;
        count = 0;
        maxLatency = 0;
        minLatency = Long.MAX_VALUE;
    }
    synchronized public void resetMaxLatency(){
        maxLatency = getMinLatency();
    }
    synchronized public void incrementPacketsReceived() {
        packetsReceived++;
    }
    synchronized public void incrementPacketsSent() {
        packetsSent++;
    }
    synchronized public void resetRequestCounters(){
        packetsReceived = 0;
        packetsSent = 0;
    }
    
    synchronized public void reset() {
        resetLatency();
        resetRequestCounters();
    }

}
