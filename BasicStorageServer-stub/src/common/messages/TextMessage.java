package common.messages;

import java.io.Serializable;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class TextMessage implements Serializable, KVMessage {

	private static final long serialVersionUID = 5549512212003782618L;
	private String msg;
	private byte[] msgBytes;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	private String key;
	private String value;
	private StatusType statusType;
	/**
	 * Constructs a TextMessage with no initial vlaues
	 */
	public TextMessage(){
		
	}
	
    /**
     * Constructs a TextMessage object with a given array of bytes that 
     * forms the message.
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public TextMessage(byte[] bytes) {
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes).trim();
		this.msgBytes = toByteArray(this.msg);
	}
	
	/**
     * Constructs a TextMessage object with a given String that
     * forms the message. 
     * 
     * @param msg the String that forms the message.
     */
	public TextMessage(String msg) {
		this.msg = msg.trim();
		this.msgBytes = toByteArray(this.msg);
	}


	/**
	 * Returns the content of this TextMessage as a String.
	 * 
	 * @return the content of this message in String format.
	 */
	public String getMsg() {
		return msg;
	}

	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes 
	 * 		in ASCII coding.
	 */
	public byte[] getMsgBytes() {
		return msgBytes;
	}
	
	private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}

	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return statusType;
	}
	
	public void setStatusType(StatusType type) {
		this.statusType = type;
	}

	public TextMessage serialize(){

		switch(statusType){
		case GET:
			return new TextMessage("{StatusType: GET, key:" + key +"}");
		case GET_ERROR:
			return new TextMessage("{StatusType: GET_ERROR, key:" + key +"}");
		case GET_SUCCESS:
			return new TextMessage("{StatusType: GET_SUCCESS, key:" + key + ", value:" + value +"}");
		case PUT:
			return new TextMessage("{StatusType: PUT, key:" + key + ", value:" + value +"}");
		case PUT_SUCCESS:
			return new TextMessage("{StatusType: PUT_SUCCESS, key:" + key + ", value:" + value +"}");
		case PUT_UPDATE:
			return new TextMessage("{StatusType: PUT_UPDATE, key:" + key + ", value:" + value +"}");
		case PUT_ERROR:
			return new TextMessage("{StatusType: PUT_ERROR, key:" + key + ", value:" + value +"}");
		case DELETE_SUCCESS:
			return new TextMessage("{StatusType: DELETE_SUCCESS, key:" + key +"}");
		case DELETE_ERROR:
			return new TextMessage("{StatusType: DELETE_ERROR, key:" + key +"}");
		default:
			return new TextMessage("{StatusType: FAILED}");
		}
	}

//	{StatusType: PUT, key:name, value: spirit}
	public TextMessage deserialize(){
		TextMessage demessage = new TextMessage();

		if(msg.charAt(0) != '{' || msg.charAt(msg.length()-1) != '}'){
			return null;
		}
		msg = msg.substring(1, msg.length()-1);
		String [] pairs = msg.split(",");
		for(String pair : pairs){
			
			String [] keyvalue= pair.split(":");
			
			if(keyvalue[0].trim().equals("StatusType")){
				demessage.setStatusType((StatusType.valueOf(keyvalue[1].trim().toUpperCase())));
			}
			else if(keyvalue[0].trim().equals("key")){
				demessage.setKey(keyvalue[1].trim());
			}
			else if(keyvalue[0].trim().equals("value")){
				if(keyvalue.length > 1){
					demessage.setValue(keyvalue[1].trim());
				}
				else{
					demessage.setValue("");
				}
			}
		}
		return demessage;

	}
	
}
