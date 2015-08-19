package com.sdw.soft.test.cooker;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

/**
 * @author shangyd
 * @date 2015年8月19日 下午5:55:39
 **/
public class ZookeeperDemo implements Watcher{

	private final CountDownLatch countdown = new CountDownLatch(1);
	@Test
	public void test01(){
		try {
			ZooKeeper zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", 3000, this);
			countdown.await();
			//zookeeper.create("/servers/app1", "app1 data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zookeeper.setData("/servers/app1", "set app1 data".getBytes(), -1);
			byte[] data = zookeeper.getData("/servers/app1", false, null);
			System.out.println("get data from node serverA -------" + new String(data));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == KeeperState.SyncConnected){
			System.out.println("connected successful ... ...");
			countdown.countDown();
		}
	}
}
