package com.sdw.soft.test.jute;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;

public class MockReqHeader implements Record{

	private long sessionId;
	private String type;
	public MockReqHeader(){}
	public MockReqHeader(long sessionId,String type){
		this.sessionId = sessionId;
		this.type = type;
	}
	
	public long getSessionId(){
		return sessionId;
	}
	
	public void setSessionId(long sessionId){
		this.sessionId = sessionId;
	}
	
	public String getType(){
		return type;
	}
	
	public void setType(String m_){
		type=m_;
	}
	
	@Override
	public void serialize(OutputArchive archive, String tag) throws IOException {
		archive.startRecord(this, tag);
		archive.writeLong(sessionId, "sessionId");
		archive.writeString(type, "type");
		archive.endRecord(this, tag);
	}

	@Override
	public void deserialize(InputArchive archive, String tag)
			throws IOException {
		archive.startRecord(tag);
		sessionId = archive.readLong("sessionId");
		type = archive.readString("type");
		archive.endRecord(tag);
	}

}
