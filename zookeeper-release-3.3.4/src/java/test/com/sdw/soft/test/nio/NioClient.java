package com.sdw.soft.test.nio;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * @author shangyd
 * @date 2015年8月7日 下午1:49:39
 **/
public class NioClient {

	//通道管理器
	private Selector selector;
	
	public NioClient init(String hostname,int port){
		//获取Socket通道
		SocketChannel channel = null;
		try {
			//获取Socket通道
			channel = SocketChannel.open();
			channel.configureBlocking(false);
			//获取通道管理器
			selector = Selector.open();
			//客户端连接服务器,需要调用channel.finishConnect();才能实际完成连接
			channel.connect(new InetSocketAddress(hostname, port));
			//为该通道注册SelectionKey.OP_CONNECT事件
			channel.register(selector, SelectionKey.OP_CONNECT);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return this;
	}
	
	
	public void listen(){
		System.out.println("client start ...");
		//轮询访问selector
		try {
			while(true){
				//选择注册过的io操作的事件(第一次为SelectionKey.OP_CONNECT)
				selector.select();
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while(it.hasNext()){
					SelectionKey key = it.next();
					//删除已选中的key,防止重复处理
					it.remove();
					if(key.isConnectable()){
						SocketChannel channel = (SocketChannel)key.channel();
						//如果正在连接,则完成连接
						if(channel.isConnectionPending()){
							channel.finishConnect();
						}
						channel.configureBlocking(false);
						//向服务器发送消息
						channel.write(ByteBuffer.wrap(new String("send message to server").getBytes()));
						//连接成功后,注册接收服务器消息事件
						channel.register(selector, SelectionKey.OP_READ);
						System.out.println("客户端连接成功!");
						//有可读数据事件
					}else if(key.isReadable()){
						SocketChannel channel = (SocketChannel) key.channel();
						ByteBuffer buffer = ByteBuffer.allocate(80);
						channel.read(buffer);
						byte[] data = buffer.array();
						String message = new String(data);
						System.out.println("receive message from server,size:" + buffer.position() + " msg:" + message);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new NioClient().init("localhost", 8000).listen();
	}
	
}
