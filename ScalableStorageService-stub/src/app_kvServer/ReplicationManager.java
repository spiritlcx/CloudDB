package app_kvServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.MessageHandler;
import common.messages.TextMessage;

public class ReplicationManager {

    private MessageHandler [] successors;
    private Logger logger;

	private static ReplicationManager replicationManager = null;
	private StorageManager storageManager;
	private Persistance persistance;
	
	public static ReplicationManager getInstance(MessageHandler [] successors, Logger logger){
		if(replicationManager == null){
			synchronized(ReplicationManager.class){
				if(replicationManager == null){
					replicationManager = new ReplicationManager(successors, logger);
				}
			}
		}
		return replicationManager;
	}
	
    public ReplicationManager(MessageHandler [] successors, Logger logger){
    	this.successors = successors;
    	this.logger = logger;
    	storageManager = StorageManager.getInstance();
    	persistance = storageManager.getPersistance();
    }

    //remove duplicates on the second successor
    public void removeRepSesuccessor(ArrayList<String> toRemove){
    	for(String key : toRemove)
    		deletes(successors[1], key);
    }
    
    public void addRepSesuccessor(HashMap<String, String> receivedPairs){
    	for(String key : receivedPairs.keySet()){
    		inserts(successors[1], key, receivedPairs.get(key));
    	}
    }
    
    
    public void replicate(StatusType type, String key, String value){
		switch(type){
		case PUT_SUCCESS:
			for(MessageHandler successor : successors)
				inserts(successor, key, value);
			break;
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
     * insert replicas to two successors
     */
    
    public void inserts(MessageHandler successor, String key, String value){
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
				logger.error(e.getMessage());
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
				logger.error(e.getMessage());
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
				logger.error(e.getMessage());
			}
    }
}
