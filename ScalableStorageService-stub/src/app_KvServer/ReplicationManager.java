package app_KvServer;

import java.io.IOException;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.MessageHandler;
import common.messages.TextMessage;

public class ReplicationManager {

    private MessageHandler [] successors;
    private Logger logger;
    
    public ReplicationManager(MessageHandler [] successors, Logger logger){
    	this.successors = successors;
    	this.logger = logger;
    }
    
    public void replicate(StatusType type, String key, String value){
		switch(type){
		case PUT_SUCCESS:
			inserts(key, value);
			break;
		case PUT_UPDATE:
			updates(key, value);
			break;
		case DELETE_SUCCESS:
			deletes(key);
			break;
		}

    }
	/**
     * insert replicas to two successors
     */
    
    public void inserts(String key, String value){
    	for(MessageHandler successor : successors){
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
    }
    

    /**
     * update replicas
     */
    
    public void updates(String key, String value){
    	for(MessageHandler successor : successors){
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
    }

    
    /**
     * delete replicas
     */
    
    public void deletes(String key){
    	for(MessageHandler successor : successors){
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
}
