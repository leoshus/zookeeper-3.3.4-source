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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;

public class ClientCnxnSocketNIO extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClientCnxnSocketNIO.class);

    private final Selector selector = Selector.open();

    private SelectionKey sockKey;

    ClientCnxnSocketNIO() throws IOException {
        super();
    }

    @Override
    boolean isConnected() {
        return sockKey != null;
    }
    
    /**
     * @return true if a packet was received
     * @throws InterruptedException
     * @throws IOException
     */
    void doIO(List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue) throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        if (sockKey.isReadable()) {//当前读事件就绪
        	//先读包的长度，一个int  
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from server sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely server has closed socket");
            }
            //如果读满，注意这里同一个包，要读2次，第一次读长度，第二次读内容，incomingBuffer重用  
            if (!incomingBuffer.hasRemaining()) {
                incomingBuffer.flip();//position=0 limit=读取完毕是的position
                //如果读的是长度  
                if (incomingBuffer == lenBuffer) {
                    recvCount++;
                    readLength();//给incomingBuffer分配包长度的空间
                } else if (!initialized) {//如果还未初始化，就是session还没建立，那server端返回的必须是ConnectResponse         
                    readConnectResult();//读取连接请求的响应信息   读取ConnectRequest，其实就是将incomingBuffer的内容反序列化成ConnectResponse对象 
                    enableRead();//将当前selectionKey 的interestOps加上OP_READ
                    if (!outgoingQueue.isEmpty()) {//如果还有写请求，确保write事件ok  
                        enableWrite();//将当前selectionKey 的interestOps加上OP_WRITE
                    }
                    lenBuffer.clear();//准备读下一个响应  
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    initialized = true;//session建立完毕  
                } else {
                    sendThread.readResponse(incomingBuffer);//读取响应数据
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                }
            }
        }
        if (sockKey.isWritable()) {//当前写事件就绪
            LinkedList<Packet> pending = new LinkedList<Packet>();
            synchronized (outgoingQueue) {
                if (!outgoingQueue.isEmpty()) {
                    updateLastSend();//修改上次发送时间
                    ByteBuffer pbb = outgoingQueue.getFirst().bb;//从outgoingQueue中取出请求Packet报文
                    sock.write(pbb);//将请求Packet报文包写入到SocketChannel
                    if (!pbb.hasRemaining()) {//pdd已经全部写入到SocketChannel 
                        sentCount++;//已发送的业务Packet数量
                        Packet p = outgoingQueue.removeFirst();//将请求Packet报文从outgoingQueue队列中移除并取 放入到pendingQueue等待服务器响应(如果是非认证和心跳请求 发送完直接扔掉)
                        if (p.requestHeader != null
                                && p.requestHeader.getType() != OpCode.ping
                                && p.requestHeader.getType() != OpCode.auth) {
                            pending.add(p);
                        }
                    }
                }
            }
            synchronized(pendingQueue) {
                pendingQueue.addAll(pending);
            }
        }
    }

    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                sock.socket().shutdownInput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown input", e);
                }
            }
            try {
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown output",
                            e);
                }
            }
            try {
                sock.socket().close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during socket close", e);
                }
            }
            try {
                sock.close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during channel close", e);
                }
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SendThread interrupted during sleep, ignoring");
            }
        }
        sockKey = null;
    }
 
    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }
            selector.close();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }
    
    /**
     * 创建一个socketChannel
     * create a socket channel.
     * @return the created socket channel
     * @throws IOException
     */
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * register with the selection and connect
     * @param sock the {@link SocketChannel} 
     * @param addr the address of remote host
     * @throws IOException
     */
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) 
    throws IOException {
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);//注册SocketChannel到Selector上 OP_CONNECTION事件 selector.select()会阻塞到OP_CONNECTION事件就绪
        boolean immediateConnect = sock.connect(addr); //连接远程zookeeper服务端
        if (immediateConnect) {//连接成功远程zookeeper服务端
            sendThread.primeConnection();//发送创建会话请求
        }
    }
    
    @Override
    void connect(InetSocketAddress addr) throws IOException {
        SocketChannel sock = createSock();//创建SocketChannel
        try {
           registerAndConnect(sock, addr);//连接并注册
        } catch (IOException e) {
            LOG.error("Unable to open socket to " + addr);
            sock.close();
            throw e;
        }
        //初始化initialized初始为false 在doIO处理中作为特殊处理第一次请求建立会话标识
        //false表示session尚未初始化
        initialized = false;

        /*
         * 重置incomingBuffer position归0  limit=capacity
         * 重置两个读buffer 准备下一次读
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getRemoteSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getRemoteSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Returns the local address to which the socket is bound.
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getLocalSocketAddress() {
        // a lot could go wrong here, so rather than put in a bunch of code
        // to check for nulls all down the chain let's do it the simple
        // yet bulletproof way
        try {
            return ((SocketChannel) sockKey.channel()).socket()
                    .getLocalSocketAddress();
        } catch (NullPointerException e) {
            return null;
        }
    }

    @Override
    synchronized void wakeupCnxn() {
        selector.wakeup();
    }
    
    @Override
    void doTransport(int waitTimeOut, List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue )
            throws IOException, InterruptedException {
        selector.select(waitTimeOut);//阻塞直到OP_CONNECT(第一次建立会话) OP_READ OP_WRITE
        Set<SelectionKey> selected;
        synchronized (this) {
            selected = selector.selectedKeys();
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here
        updateNow();
        for (SelectionKey k : selected) {
            SocketChannel sc = ((SocketChannel) k.channel());
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {//OP_CONNECT 事件就绪
                if (sc.finishConnect()) {
                    updateLastSendAndHeard();
                    sendThread.primeConnection();
                }
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {//OP_READ或OP_WRITE事件就绪
                doIO(pendingQueue, outgoingQueue);//发送或读取相应内容
            }
        }
        if (sendThread.getZkState().isConnected()) {//如果已经建立连接
            synchronized(outgoingQueue) {
                if (!outgoingQueue.isEmpty()) {//outgoing为empty则为通道添加OP_WRITE事件监听
                    enableWrite();
                } else {
                    disableWrite();//否则移除OP_WRITE事件监听
                }
            }
        }
        selected.clear();
    }

    //TODO should this be synchronized?
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        ((SocketChannel) sockKey.channel()).socket().close();
    }

    @Override
    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    private synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    synchronized private void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    synchronized void enableReadWriteOnly() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }
}
