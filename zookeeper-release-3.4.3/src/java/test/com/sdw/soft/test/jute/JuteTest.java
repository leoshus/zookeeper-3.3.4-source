package com.sdw.soft.test.jute;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.junit.Test;

public class JuteTest {

	
	@Test
	public void serializeTest(){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
		try {
			//开始序列化
			new MockReqHeader(0x64451eccb92a34el,"ping").serialize(boa, "header");
			ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());//这里通常是TCP网络传输的对象
			
			//开始反序列化
			ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
			BinaryInputArchive bia = BinaryInputArchive.getArchive(bbis);
			MockReqHeader mock = new MockReqHeader();
			mock.deserialize(bia, "header");
			System.out.println(mock.getSessionId() + "--------------" + mock.getType());
			bbis.close();
			baos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
