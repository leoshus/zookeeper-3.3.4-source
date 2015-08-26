package com.sdw.soft.test.nio;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class SocketChannelDemo {

	public static void main(String[] args) throws Exception {
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);
		socketChannel.connect(new InetSocketAddress("127.0.0.1", 8989));
		Selector selector = Selector.open();
		socketChannel.register(selector, SelectionKey.OP_CONNECT);
		while(true){
			selector.select();
			Iterator iter = selector.selectedKeys().iterator();
			while(iter.hasNext()){
				SelectionKey selectionKey = (SelectionKey) iter.next();
				iter.remove();
				if(selectionKey.isConnectable()){
					SocketChannel channel = (SocketChannel) selectionKey.channel();
					if(channel.isConnectionPending()){//如果正在连接，则完成连接
						channel.finishConnect();
					}
					channel.configureBlocking(false);
					channel.write(ByteBuffer.wrap("hello java NIO server".getBytes()));
					channel.register(selector, SelectionKey.OP_READ);//在和服务端连接成功之后，为了可以接收到服务端的信息，需要给通道设置读的权限。
				}else if(selectionKey.isReadable()){
					 SocketChannel channel = (SocketChannel) selectionKey.channel();
					 ByteBuffer buffer = ByteBuffer.allocate(1024);
					 channel.read(buffer);
					 String msg = new String(buffer.array());
					 System.out.println("client receive the msg =" + msg);
					 channel.register(selector, SelectionKey.OP_WRITE);
				}else if(selectionKey.isWritable()){
					SocketChannel channel = (SocketChannel)selectionKey.channel();
					channel.write(ByteBuffer.wrap("咱们开始谈话吧".getBytes()));
					channel.register(selector, SelectionKey.OP_READ);
				}
			}
			ByteBuffer buffer = ByteBuffer.allocate(1024);
			buffer.clear();
			buffer.put("Hello Java NIO".getBytes());
			buffer.flip();
			while(buffer.hasRemaining())
				socketChannel.write(buffer);
		}
	}
}
