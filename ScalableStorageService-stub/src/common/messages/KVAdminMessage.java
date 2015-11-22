package common.messages;

import metadata.Metadata;

public class KVAdminMessage {
	public enum StatusType {
		INIT,
		START,
		STOP,
		SHUTDOWN,
		MOVE,
		UPDATE
	}
		
	private StatusType type;
	private int cacheSize;
	private String displacementStrategy;
	private Metadata metadata;
	
	public void setStatus(StatusType type){
		this.type = type;
	}
	
	public void setCacheSize(int cacheSize){
		this.cacheSize = cacheSize;
	}
	
	public void setDisplacementStrategy(String displacementStrategy){
		this.displacementStrategy = displacementStrategy;
	}
	
	public void setMetadata(Metadata metadata){
		this.metadata = metadata;
	}
	
	public String serialize(){
		if(type == null)
			return null;
		switch(type){
		case START:
		case STOP:
		case SHUTDOWN:
			return "{StatusType: " + type +"}";
		
		case INIT:
			return "{StatusType: " + type +", cacheSize: " + cacheSize + ", displacementStrategy: " +
		displacementStrategy + ", metadata: " + metadata + "}";
			
			
		default:
			return null;
		}
	}
}
