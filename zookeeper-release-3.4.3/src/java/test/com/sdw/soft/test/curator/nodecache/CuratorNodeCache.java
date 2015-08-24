package com.sdw.soft.test.curator.nodecache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class CuratorNodeCache {

	private String path = "/curator/nodecahe";
	private String path1 = "/curator";
	private CuratorFramework client = null;
	private TestingServer server = null;
	
	/**
	 * NodeCacheListener
	 */
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
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, "curator node cache".getBytes());
		    final NodeCache nodecache = new NodeCache(client,path,false);
		    nodecache.start(true);
		    nodecache.getListenable().addListener(new NodeCacheListener(){
				@Override
				public void nodeChanged() throws Exception {
					System.out.println("Node data update ,new data=" + new String(nodecache.getCurrentData().getData()));
				}
		    });
		    
		    client.setData().forPath(path,"update data".getBytes());
		    Thread.sleep(1000);
		    client.delete().deletingChildrenIfNeeded().forPath(path);
		    Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			CloseableUtils.closeQuietly(client);
		}
	}
	
	/**
	 * PathChildrenCache
	 */
	@Test
	public void test02(){
		try {
			server = new TestingServer();
			client = CuratorFrameworkFactory.builder()
					.connectString(server.getConnectString())
					.sessionTimeoutMs(5000)
					.retryPolicy(new ExponentialBackoffRetry(1000, 3))
					.build();
			client.start();
			
			PathChildrenCache cache = new PathChildrenCache(client,path1,true);
			cache.start(StartMode.POST_INITIALIZED_EVENT);
			cache.getListenable().addListener(new PathChildrenCacheListener(){
				@Override
				public void childEvent(CuratorFramework client,
						PathChildrenCacheEvent event) throws Exception {
					switch(event.getType()){
					case CHILD_ADDED:
						System.out.println("CHILD_ADDED ," + event.getData().getPath());
						break;
					case CHILD_UPDATED:
						System.out.println("CHILD_UPDATED ," + event.getData().getPath());
						break;
					case CHILD_REMOVED:
						System.out.println("CHILD_REMOVED ," + event.getData().getPath());
						break;
					default:
							break;
					}
				}
			});
			
			client.create().withMode(CreateMode.PERSISTENT).forPath(path1);
			Thread.sleep(1000);
			
			client.create().withMode(CreateMode.PERSISTENT).forPath(path1 + "/cachenode");
			Thread.sleep(1000);
			client.delete().forPath(path1 + "/cachenode");
			Thread.sleep(1000);
			client.delete().forPath(path1);
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			CloseableUtils.closeQuietly(client);
		}
	}
}
