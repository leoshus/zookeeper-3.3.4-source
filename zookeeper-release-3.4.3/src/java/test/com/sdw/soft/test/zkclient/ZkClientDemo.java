package com.sdw.soft.test.zkclient;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

/**
 * ZKClient 
 * @author Administrator
 *
 */
public class ZkClientDemo {

	private static ZkClient zkclient = null;
	
	@Test
	public void test001(){
		zkclient = new ZkClient("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", 50000);
		int i = 0;
		while(true){
			zkclient.createEphemeral("/testzk" + i);//递归创建节点  true表示递归创建节点
			i++;
		}
	}
	/**
	 * 使用ZKClient 创建ZooKeeper客户端
	 */
	@Test
	public void test01(){
		zkclient = new ZkClient("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", 50000);
		System.out.println("ZooKeeper session established.");
		zkclient.createPersistent("/servers/zk1", true);//递归创建节点  true表示递归创建节点
	}
	/**
	 * 使用ZkClient获取子节点列表
	 */
	@Test
	public void test02() throws Exception{
		String path = "/zkclient";
		ZkClient zkclient = new ZkClient("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", 5000);
		zkclient.subscribeChildChanges(path, new IZkChildListener() {//注册listener 非zookeeper原生API   不是一次性的
			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds)
					throws Exception {
				System.out.println(parentPath + " 's child changed,currentChilds:" + currentChilds);
			}
		});
		zkclient.createPersistent(path);
		Thread.sleep(1000);
		System.out.println(zkclient.getChildren(path));
		Thread.sleep(1000);
		zkclient.createEphemeral(path + "/c1");
		Thread.sleep(1000);
		zkclient.delete(path + "/c1");
		Thread.sleep(1000);
		zkclient.delete(path);
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	
	/**
	 * ZKClient 获取节点数据内容
	 */
	@Test
	public void test03() throws Exception{
		String path = "/zkclient";
		ZkClient zkclient = new ZkClient("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", 5000);
		zkclient.createEphemeral(path, "123");
		//注册节点数据变化watcher 非一次性
		zkclient.subscribeDataChanges(path, new IZkDataListener(){

			@Override
			public void handleDataChange(String dataPath, Object data)//节点内容变更
					throws Exception {
				System.out.println("Node " + dataPath + " changed, new data:" + data);
			}

			@Override
			public void handleDataDeleted(String dataPath) throws Exception {//节点删除
				System.out.println("Node " + dataPath +" deleted.");
			}
			
		});
		System.out.println(zkclient.readData(path));
		zkclient.writeData(path, "456");
		Thread.sleep(1000);
		zkclient.delete(path);
		Thread.sleep(Integer.MAX_VALUE);
	}
	
}
