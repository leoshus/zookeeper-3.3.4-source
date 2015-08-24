package com.sdw.soft.test.curator;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

/**
 * Curator demo
 * @author Administrator
 *
 */
public class CuratorDemo {

	/**
	 * Curator 创建一个zookeeper客户端
	 */
	@Test
	public void test01() throws Exception{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", retryPolicy);
		client.start();
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	/**
	 * 使用Fluent风格的API创建Zookeeper客户端
	 * @throws Exception
	 */
	@Test
	public void test02() throws Exception{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183")
				.sessionTimeoutMs(5000)
				.retryPolicy(retryPolicy)
				.build();
		client.start();
		Thread.sleep(Integer.MAX_VALUE);
		
	}
	
	/**
	 * 使用curator-test 测试curator创建zookeeper客户端 存取数据
	 * @date 2015年8月24日 下午2:02:33
	 * 
	 * @throws Exception
	 */
	@Test
	public void test03() throws Exception {
		String path = "/example/basic";
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		try {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy);
			client.start();
			client.create().creatingParentsIfNeeded().forPath(path, "Hello World!".getBytes());
			CloseableUtils.closeQuietly(client);
			
			client = CuratorFrameworkFactory.builder()
					.connectString(server.getConnectString())
					.sessionTimeoutMs(3000)
					.retryPolicy(retryPolicy)
					.build();
			client.start();
			System.out.println(new String(client.getData().forPath(path)));
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}
	
	/**
	 * Curator API demo
	 * @throws Exception 
	 * @date 2015年8月24日 下午2:38:10
	 */
	@Test
	public void test04(){
		String path = "/example/test04";
		CuratorFramework client = null;
		try {
			TestingServer server = new TestingServer();
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			client = CuratorFrameworkFactory.builder()
					.connectString(server.getConnectString())
					.sessionTimeoutMs(3000)
					.retryPolicy(retryPolicy)
					.build();
			client.start();
			client.create().forPath(path);//创建节点
			client.create().withMode(CreateMode.EPHEMERAL).forPath(path,"".getBytes());//创建临时节点
			client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, "".getBytes());//创建临时序列节点
			client.setData().forPath(path, "".getBytes());//设置节点数据
			
			CuratorListener curatorListener = new CuratorListener() {
				@Override
				public void eventReceived(CuratorFramework client, CuratorEvent event)
						throws Exception {
					
				}
			};
			
			client.getCuratorListenable().addListener(curatorListener);
			client.setData().inBackground().forPath(path, "".getBytes());//异步设置节点数据
			BackgroundCallback backgroundCallback = new BackgroundCallback() {
				
				@Override
				public void processResult(CuratorFramework client, CuratorEvent event)
						throws Exception {
					
				}
			};
			client.setData().inBackground(backgroundCallback).forPath(path, "".getBytes());//带回调的异步设置节点数据
			
			client.delete().forPath(path);//删除节点
			client.delete().guaranteed().forPath(path);//删除节点 保证完成
			client.getChildren().watched().forPath(path);//获取指定节点的子节点 并设置监听
			client.getChildren().usingWatcher(new Watcher(){
				@Override
				public void process(WatchedEvent event) {
					
				}}).forPath(path);//获取指定节点的子节点 并设置监听
			
			//-----------------------------事务------------------------------------
			Collection<CuratorTransactionResult> results = client.inTransaction().create().forPath(path,"".getBytes())
					.and().setData().forPath("/curator/zk1","".getBytes())
					.and().delete().forPath("/curator/zk1")
					.and().commit();
			for(CuratorTransactionResult result : results){
				System.out.println(result.getForPath() + "=====" + result.getType());
			}
			//start the transaction builder
			CuratorTransaction transaction = client.inTransaction();
			//add a delete operation
			CuratorTransactionFinal transactionFinal = transaction.delete().forPath(path).and();
			//commit the transaction
			transactionFinal.commit();
					
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			CloseableUtils.closeQuietly(client);
		}
	}
	
	/**
	 * 
	 * @date 2015年8月24日 下午3:13:07
	 */
	@Test
	public void test05(){
		CuratorFramework client = null;
		PersistentEphemeralNode node = null;
		String path = "/curator/persistenttest05";
		TestingServer server = null;
		try {
			server = new TestingServer();
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(3000, 3));
			client.getConnectionStateListenable().addListener(new ConnectionStateListener(){
				@Override
				public void stateChanged(CuratorFramework client, ConnectionState newState) {
					System.out.println("client state =" + newState.name());
				}});
			client.start();
			
			node = new PersistentEphemeralNode(client, Mode.EPHEMERAL, "/curator/test05", "hello test05".getBytes());
			node.start();
			node.waitForInitialCreate(3, TimeUnit.SECONDS);
			String actualPath = node.getActualPath();
			System.out.println("node =" + actualPath + ",value=" + new String(client.getData().forPath(actualPath)));
			
			client.create().forPath(path, "persistent test05".getBytes());
			System.out.println("node /curator/persistenttest05 value=" + new String(client.getData().forPath(path)));
			KillSession.kill(client.getZookeeperClient().getZooKeeper(), server.getConnectString());
			System.out.println("node " + actualPath + " doesn't exist : " + (client.checkExists().forPath(actualPath) == null) + "," + (client.checkExists().forPath(actualPath)));
			System.out.println("node " + path + " vlaue=" + new String(client.getData().forPath(path)));
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			CloseableUtils.closeQuietly(node);
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}
	
	
}
