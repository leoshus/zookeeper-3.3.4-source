package com.sdw.soft.test.cooker;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * @author shangyd
 * @date 2015年8月19日 下午5:55:39
 **/
public class ZookeeperDemo implements Watcher{
	
	private static final int TIME_OUT = 3000;
	private final CountDownLatch countdown = new CountDownLatch(1);
	private ZooKeeper zookeeper = null;
	private Stat stat = new Stat();
	@Test
	public void test01(){
		try {
			ZooKeeper zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", TIME_OUT, this);
			countdown.await();
			long sessionId = zookeeper.getSessionId();
			byte[] sessionPasswd = zookeeper.getSessionPasswd();
//			String path = zookeeper.create("/servers/app1", "app1 data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//			System.out.println(path);
			zookeeper.setData("/servers/app1", "set app1 data".getBytes(), -1);
			byte[] data = zookeeper.getData("/servers/app1", false, null);
			System.out.println("get data from node serverA -------" + new String(data));
			
			//合法复用会话
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183", TIME_OUT, this, sessionId, sessionPasswd);
			
			//使用异步接口
			zookeeper.create("/servers/app2", "test app2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new ExtStringCallback(), "I am context");
			//删除节点
			zookeeper.delete("/servers/app2", -1, new ExtVoidCallback(), "I am context");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * getChildren
	 */
	@Test
	public void test02(){
		try {
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			countdown.await();
			zookeeper.create("/servers/app3", "test app3".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zookeeper.getChildren("/servers", true, new ExtChildren2Callback(), "I am context");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * getData
	 */
	@Test
	public void test03(){
		try {
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			countdown.await();
			System.out.println(zookeeper.getData("/servers/app3", true, stat));
			System.out.println("创建节点时的事务ID="+stat.getCzxid() + ",最近修改节点的事务ID=" + stat.getMzxid()+","+stat.getVersion());
			zookeeper.setData("/servers/app3", "update app3".getBytes(), -1);
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * setData
	 */
	@Test
	public void test04(){
		try {
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			countdown.await();
			String path = zookeeper.create("/servers/app4", "test app4".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println(zookeeper.getData("/servers/app4", true, null));
			Stat stat = zookeeper.setData("/servers/app4", "update app4 first".getBytes(), -1);
			System.out.println("first update data result =" + stat.getCzxid() + "," +stat.getMzxid() + "," +stat.getVersion());
			Stat stat2 = zookeeper.setData("/servers/app4", "update app4 second".getBytes(), stat.getVersion());
			System.out.println("second update data result =" + stat2.getCzxid() + "," + stat2.getMzxid() + "," + stat.getVersion());
			try {
				zookeeper.setData("/servers/app4", "third update app4".getBytes(), stat.getVersion());
			} catch (KeeperException e) {
				e.printStackTrace();
				System.out.println(e.code()+ "," + e.getMessage());
			}
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 异步接口 setData
	 */
	@Test
	public void test05(){
		try {
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			countdown.await();
			String path = zookeeper.create("/servers/app5", "test app5".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zookeeper.setData("/servers/app5", "test app5".getBytes(), -1, new ExtStatCallback(), "I am context");
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * exists
	 */
	@Test
	public void test06(){
		try {
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtExistsWatcher());
			countdown.await();
			zookeeper.exists("/servers/app6", true);
			zookeeper.create("/servers/app6", "test app6".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zookeeper.setData("/servers/app6", "update app6".getBytes(), -1);
			zookeeper.create("/servers/app6/web", "app6 web".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			zookeeper.delete("/servers/app6/web", -1);
			zookeeper.delete("/servers/app6", -1);
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * ACL  degist方式
	 */
	@Test
	public void test07(){
		try {
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			countdown.await();
			zookeeper.addAuthInfo("digest", "foo:true".getBytes());
			zookeeper.create("/servers/app7", "test app7".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
			zookeeper.getData("/servers/app7", false, null);
			ZooKeeper zookeeper2 = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			zookeeper2.addAuthInfo("digest", "foo:false".getBytes());
			zookeeper2.getData("/servers/app7", false, null);
			Thread.sleep(Integer.MAX_VALUE);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * ACL 对于delete特殊情况
	 */
	@Test
	public void test08(){
		try {
			zookeeper = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			countdown.await();
			zookeeper.addAuthInfo("digest", "foo:true".getBytes());
			zookeeper.create("/servers/app8", "test app8".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
			zookeeper.create("/servers/app9", "test app9".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
			ZooKeeper zookeeper2 = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			zookeeper2.addAuthInfo("digest", "foo:false".getBytes());
			zookeeper2.delete("/servers/app8", -1);//删除失败
			
			zookeeper.delete("/servers/app8", -1);//删除成功
			ZooKeeper zookeeper3 = new ZooKeeper("192.168.183.133:2181,192.168.183.133:2182,192.168.183.133:2183",TIME_OUT,new ExtWatcher());
			zookeeper3.delete("/servers/app9", -1);//删除成功
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * exists watcher
	 * @author Administrator
	 *
	 */
	class ExtExistsWatcher implements Watcher{

		@Override
		public void process(WatchedEvent event) {
			try {
				if(KeeperState.SyncConnected == event.getState()){
					if(Event.EventType.None == event.getType() && null == event.getPath()){
						countdown.countDown();
					}else if(EventType.NodeCreated == event.getType()){
						System.out.println("Node(" + event.getPath() +") created");
						zookeeper.exists(event.getPath(), true);
					}else if (EventType.NodeDeleted == event.getType()){
						System.out.println("Node(" + event.getPath() +") deleted");
						zookeeper.exists(event.getPath(), true);
					}else if(EventType.NodeDataChanged == event.getType()){
						System.out.println("Node (" + event.getPath() + ") data changed" );
						zookeeper.exists(event.getPath(), true);
					}
				}
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	class ExtWatcher implements Watcher{

		@Override
		public void process(WatchedEvent event) {
			if(KeeperState.SyncConnected == event.getState()){
				if(EventType.None == event.getType() && null == event.getPath()){
					countdown.countDown();
				}else if(event.getType() == EventType.NodeChildrenChanged){
					try {//NodeChildrenChanged事件通知 仅仅发出一个通知并不会将节点变化情况发送给客户端 需要客户端自己重新获取
						System.out.println("ReGet child:" + zookeeper.getChildren(event.getPath(), true));
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}else if(event.getType() == EventType.NodeDataChanged){//setData 数据内容和版本发生变化都会触发NodeDataChanged
					try {
						System.out.println(zookeeper.getData("/servers/app3", true, stat));
						System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + stat.getVersion());
					} catch (KeeperException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
	/**
	 * setData 异步接口
	 * @author Administrator
	 *
	 */
	class ExtStatCallback implements AsyncCallback.StatCallback{

		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			System.out.println("set data result=" + rc + "," + path + "," + ctx + "," + stat);
		}
		
	}
	/**
	 * 获取子节点列表异步接口
	 * @author Administrator
	 *
	 */
	class ExtChildren2Callback implements AsyncCallback.Children2Callback{

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children, Stat stat) {
			System.out.println("get children node result=" + rc + "," + path + "," + ctx + "," + children + "," + stat);
		}
		
	}
	@Override
	public void process(WatchedEvent event) {
		if(event.getState() == KeeperState.SyncConnected){
			System.out.println("connected successful ... ...");
			countdown.countDown();
		}
	}
	
	
	class ExtStringCallback implements AsyncCallback.StringCallback {
		/**
		 * @param rc resultCode 0 ok -4 ConnectionLoss -110 NodeExists -112 SessionExpired
		 * @param path 接口调用时传入API的数据节点路径参数值
		 * @param ctx  接口调用时传入API的ctx值
		 * @param name  服务端返回当前数据节点的完整节点路径
		 */
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			System.out.println("create path result =[" + rc + "," + path + "," + ctx +",rel path name =" + name);
		}
		
	}
	
	
	class ExtVoidCallback implements AsyncCallback.VoidCallback {

		@Override
		public void processResult(int rc, String path, Object ctx) {
			System.out.println("delete path result =" + rc + "," + path + "," + ctx);
		}
	}
}
