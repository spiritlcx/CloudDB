package app_kvServer;

import java.io.IOException;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.MessageHandler;
import common.messages.TextMessage;

public class ReplicationManager {

    private MessageHandler [] successors = null;

	private static ReplicationManager replicationManager = null;
	
	public static ReplicationManager getInstance(){
		return replicationManager;
	}

	public static ReplicationManager getInstance(MessageHandler [] successors){
		if(replicationManager == null){
			synchronized(ReplicationManager.class){
				if(replicationManager == null){
					replicationManager = new ReplicationManager(successors);
				}
			}
		}
		return replicationManager;
	}
	
    public ReplicationManager(MessageHandler [] successors){
    	this.successors = successors;
    }

    public void replicate(StatusType type, String key, String value){
		switch(type){
		case PUT_SUCCESS:
		case PUT_UPDATE:
			for(MessageHandler successor : successors)
				updates(successor, key, value);
			break;
		case DELETE_SUCCESS:
			for(MessageHandler successor : successors)
				deletes(successor, key);
			break;
		default:
			break;
		}

    }

    /**
     * update replicas
     */
    
    public void updates(MessageHandler successor, String key, String value){
    		if(successor == null)
    			return;

    		TextMessage sentMessage = new TextMessage();
    		sentMessage.setStatusType(KVMessage.StatusType.PUT);
    		sentMessage.setKey(key);
    		sentMessage.setValue(value);    	
    		sentMessage = sentMessage.serialize();
    		try {
				successor.sendMessage(sentMessage.getMsg());
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
    }
    
    /**
     * delete replicas
     */
    
    public void deletes(MessageHandler successor, String key){
    		if(successor == null)
    			return;

    		TextMessage sentMessage = new TextMessage();
    		sentMessage.setStatusType(KVMessage.StatusType.PUT);
    		sentMessage.setKey(key);
    		sentMessage.setValue("null");
    		sentMessage = sentMessage.serialize();
    		try {
				successor.sendMessage(sentMessage.getMsg());
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
    }
}