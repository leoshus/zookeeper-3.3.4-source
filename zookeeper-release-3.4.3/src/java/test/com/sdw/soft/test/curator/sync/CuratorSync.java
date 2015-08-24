package com.sdw.soft.test.curator.sync;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class CuratorSync {
	String path = "/curatorsync";
	private TestingServer server = null;
	CuratorFramework client = null;
    private static CountDownLatch countdown = new CountDownLatch(2);
    private static ExecutorService es = Executors.newFixedThreadPool(2);
	@Test
	public void test01(){
		try {
			server = new TestingServer();
			client = CuratorFrameworkFactory.builder()
					.connectString(server.getConnectString())
					.sessionTimeoutMs(5000)
					.retryPolicy(new ExponentialBackoffRetry(1000, 3))
					.build();
			client.start();
			System.out.println("main thread " + Thread.currentThread().getName() );
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new BackgroundCallback(){

				@Override
				public void processResult(CuratorFramework client,
						CuratorEvent event) throws Exception {
					System.out.println("event[code=" + event.getResultCode() + ",type=" + event.getType() + "]");
					System.out.println("Thread of processResult=" + Thread.currentThread().getName());
					countdown.countDown();
				}
				
			},es).forPath(path,"curator sync".getBytes());//使用自定义的线程池处理节点创建
			
			//不适用自定义的线程池
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new BackgroundCallback(){
				@Override
				public void processResult(CuratorFramework client,
						CuratorEvent event) throws Exception {
					System.out.println("event [code=" + event.getResultCode() + " ,type=" + event.getType() + "]");
					System.out.println("Thread of processResult=" + Thread.currentThread().getName());
					countdown.countDown();
				}
			}).forPath(path, "haha".getBytes());
			countdown.await();
			es.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			CloseableUtils.closeQuietly(client);
		}
		
	}
}
