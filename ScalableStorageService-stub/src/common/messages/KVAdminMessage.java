package common.messages;

import java.io.UnsupportedEncodingException;

import ecs.Server;
import metadata.Metadata;


public class KVAdminMessage extends Message{
	public enum StatusType {
		INIT,
		START,
		STOP,
		WRITELOCK,
		RECEIVED,
		SHUTDOWN,
		MOVE, // successor will move data to processor
		RECEIVE, // processor will prepare to receive data
		REMOVE,
		MOVEFINISH,
		DATA,
		UPDATE,
		PREPARED
	}
		
	private String msg;
	private byte[] msgBytes;

	private StatusType type;
	private int cacheSize;
	private String displacementStrategy;
	private Metadata metadata;
	private String ip;
	private int port;
	private String from;
	private String to;
	private String data;
	
	public KVAdminMessage(){
		
	}
	public KVAdminMessage(String msg){
		this.msg = msg.trim();
		this.msgBytes = toByteArray(this.msg);
		try {
			this.msg = new String(msgBytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public KVAdminMessage(byte [] bytes){
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes).trim();
		this.msgBytes = toByteArray(this.msg);
	}
	
	public KVAdminMessage serialize(){
		if(type == null)
			return null;
		switch(type){
		case START:
		case STOP:
		case SHUTDOWN:
		case WRITELOCK:
		case RECEIVE:
		case PREPARED:
		case MOVEFINISH:
			return new KVAdminMessage("{StatusType: " + type +"}");
		case RECEIVED:
			return new KVAdminMessage("{StatusType: " + type + ", port: " + port + "}");
		case INIT:
			return new KVAdminMessage("{StatusType: " + type +", cacheSize: " + cacheSize + ", displacementStrategy: " +
		displacementStrategy + ", metadata: " + metadata + "}");

		case DATA:
			return new KVAdminMessage("{StatusType: " + type + ", data: " + data + "}");
		case MOVE:
			return new KVAdminMessage("{StatusType: " + type + ", from: " + from + ", to: " + to + ", ip: " + ip + ", port: " + port + "}");
		case REMOVE:
			return new KVAdminMessage("{StatusType: " + type + ", from: " + from + ", to: " + to + "}");			
		case UPDATE:
			return new KVAdminMessage("{StatusType: " + type + ", metadata: " + metadata + "}");
			
		default:
			return null;
		}
	}

	public KVAdminMessage deserialize(String msg){
		KVAdminMessage demessage = new KVAdminMessage(msg);
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
				
				String[] datasets = keyvalue[1].trim().split("<");
				String[] serverset;
					
				for(String dataset: datasets){

					serverset = dataset.substring(1, dataset.length()-1).split(" ");
					
					if(serverset.length == 5){
						metadata.add(new Server(serverset[0], serverset[1], serverset[2], serverset[3], serverset[4]));
					}
				}

				demessage.setMetadata(metadata);
				
			}else if(keyvalue[0].trim().equals("data")){
				demessage.setData(keyvalue[1].trim());
			}else if(keyvalue[0].trim().equals("from")){
				demessage.setFrom(keyvalue[1].trim());
			}else if(keyvalue[0].trim().equals("to")){
				demessage.setTo(keyvalue[1].trim());
			}else if(keyvalue[0].trim().equals("ip")){
				demessage.setIp(keyvalue[1].trim());
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
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public byte[] getMsgBytes() {
		return msgBytes;
	}
	public void setMsgBytes(byte[] msgBytes) {
		this.msgBytes = msgBytes;
	}
	public StatusType getType() {
		return type;
	}
	public void setType(StatusType type) {
		this.type = type;
	}
	public String getFrom() {
		return from;
	}
	public void setFrom(String from) {
		this.from = from;
	}
	public String getTo() {
		return to;
	}
	public void setTo(String to) {
		this.to = to;
	}
	public String getData() {
		return data;
	}
	public void setData(String data) {
		this.data = data;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}

}
