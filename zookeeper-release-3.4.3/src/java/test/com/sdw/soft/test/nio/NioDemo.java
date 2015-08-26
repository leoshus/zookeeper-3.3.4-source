package com.sdw.soft.test.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class NioDemo {

	public static void main(String[] args) throws IOException{
		 ServerSocketChannel serverSocket = ServerSocketChannel.open();
		 serverSocket.bind(new InetSocketAddress(8989));
		// 设置通道为非阻塞
		 serverSocket.configureBlocking(false);
		 //获取一个通道管理器
		 Selector selector = Selector.open();
		 //将通道管理器和该通道绑定，并为该通道注册SelectionKey.OP_ACCEPT事件,注册该事件后，
		  //当该事件到达时，selector.select()会返回，如果该事件没到达selector.select()会一直阻塞。
		 serverSocket.register(selector, SelectionKey.OP_ACCEPT);
		 
		 System.out.println("server start successfully!");
		 while(true){//轮询监听selector上是否存在准备就绪的事件
			 selector.select();//注册事件就绪时返回  否则一直阻塞
			 //获得Selector中选中项的迭代器 选中项为注册事件
			 Iterator iter = selector.selectedKeys().iterator();
			 while(iter.hasNext()){
				 SelectionKey selectionKey = (SelectionKey) iter.next();
				 iter.remove();//删除已选selectionKey 防止重复处理
				 if(selectionKey.isAcceptable()){//客户端请求连接事件
					ServerSocketChannel server = (ServerSocketChannel)selectionKey.channel();
					SocketChannel socketChannel = server.accept();//获得和客户端连接的通道
					socketChannel.configureBlocking(false);//设置为非阻塞
					socketChannel.write(ByteBuffer.wrap("from server,  connect successfully .".getBytes()));
					socketChannel.register(selector, SelectionKey.OP_READ);//在和客户端连接成功之后，为了可以接收到客户端的信息，需要给通道设置读的权限
				 }else if(selectionKey.isReadable()){
					 SocketChannel channel = (SocketChannel) selectionKey.channel();
					 ByteBuffer buffer = ByteBuffer.allocate(1024);
					 channel.read(buffer);
					 String msg = new String(buffer.array());
					 System.out.println("server receive the msg =" + msg);
					 channel.write(ByteBuffer.wrap(msg.getBytes()));//将消息回写到客户端
					 channel.register(selector, SelectionKey.OP_WRITE);
				 }else if(selectionKey.isWritable()){
					 SocketChannel channel = (SocketChannel)selectionKey.channel();
					 channel.write(ByteBuffer.wrap("server say:你先说吧".getBytes()));
					 channel.register(selector, SelectionKey.OP_READ);
				 }
			 }
		 }
	}
}
