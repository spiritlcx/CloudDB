package common.messages;

import metadata.Metadata;


public class KVAdminMessage {
	public enum StatusType {
		INIT,
		START,
		STOP,
		RECEIVED,
		SHUTDOWN,
		MOVE,
		UPDATE
	}
		
	private StatusType type;
	private int cacheSize;
	private String displacementStrategy;
	private Metadata metadata;
	private int port;
		
	public String serialize(){
		if(type == null)
			return null;
		switch(type){
		case START:
		case STOP:
		case SHUTDOWN:
			return "{StatusType: " + type +"}";
		case RECEIVED:
			return "{StatusType: " + type + ", port: " + port + "}";
		case INIT:
			return "{StatusType: " + type +", cacheSize: " + cacheSize + ", displacementStrategy: " +
		displacementStrategy + ", metadata: " + metadata + "}";
		
		case MOVE:
			return "";
			
		case UPDATE:
			return "";
			
		default:
			return null;
		}
	}

	public KVAdminMessage deserialize(String msg){
		KVAdminMessage demessage = new KVAdminMessage();
		msg = msg.trim();

		if(msg.charAt(0) != '{' || msg.charAt(msg.length()-1) != '}'){
			return null;
		}
		msg = msg.substring(1, msg.length()-1);
		
		String [] pairs = msg.split(",");

		for(String pair : pairs){
			
			String [] keyvalue= pair.split(":");
			
			if(keyvalue[0].trim().equals("StatusType")){
				demessage.setStatusType((StatusType.valueOf(keyvalue[1].trim().toUpperCase())));
			}else if(keyvalue[0].trim().equals("cacheSize")){
				demessage.setCacheSize(Integer.parseInt(keyvalue[1].trim()));
			}else if(keyvalue[0].trim().equals("port")){
				demessage.setPort(Integer.parseInt(keyvalue[1].trim()));
			}else if(keyvalue[0].trim().equals("displacementStrategy")){
				demessage.setDisplacementStrategy(keyvalue[1].trim());
			}else if(keyvalue[0].trim().equals("metadata")){
				Metadata metadata = new Metadata();
				
				demessage.setMetadata(metadata);
			}
		}

		return demessage;
	}
	
	public StatusType getStatusType(){
		return type;
	}
	
	public void setStatusType(StatusType type){
		this.type = type;
	}
	
	public int getCacheSize(){
		return cacheSize;
	}
	
	public void setCacheSize(int cacheSize){
		this.cacheSize = cacheSize;
	}

	public String getDisplacementStrategy(){
		return displacementStrategy;
	}
	
	public void setDisplacementStrategy(String displacementStrategy){
		this.displacementStrategy = displacementStrategy;
	}

	public Metadata getMetadata(){
		return metadata;
	}
	public void setMetadata(Metadata metadata){
		this.metadata = metadata;
	}

	public int getPort(){
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
}
