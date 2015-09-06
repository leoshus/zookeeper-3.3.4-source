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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 * Leader服务器的事务投票处理器 也是Leader服务器事务处理流程的发起者
 * 1、对于非事务请求 ProposalRequestProcessor会直接将请求流转到CommitProcessor处理器  不再做其他处理
 * 2、对于事务请求  ProposalReqeustProcessor除了将请求流转给CommitProcessor处理器外,还会根据请求类型创建对应的Proposal提议
 *     并发送给所有的Follower服务器来发起一次集群内的事务投票
 *    同时ProposalRequestProcessor还会将事务请求交付给SyncRequestProcessor进行事务日志的记录
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG =
        LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;
    
    RequestProcessor nextProcessor;

    SyncRequestProcessor syncProcessor;

    public ProposalRequestProcessor(LeaderZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
    }
    
    /**
     * initialize this processor
     */
    public void initialize() {
        syncProcessor.start();
    }
    
    public void processRequest(Request request) {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");
                
        
        /* In the following IF-THEN-ELSE block, we process syncs on the leader. 
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't 
         * contain the handler. In this case, we add it to syncHandler, and 
         * call processRequest on the next processor.
         */
        
        if(request instanceof LearnerSyncRequest){
            zks.getLeader().processSync((LearnerSyncRequest)request);
        } else {
                nextProcessor.processRequest(request);
            if (request.hdr != null) {
                // We need to sync and get consensus on any transactions
                zks.getLeader().propose(request);
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
