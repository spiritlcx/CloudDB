package common.messages;

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
	public void setStatus(StatusType type){
		this.type = type;
	}
	
}
