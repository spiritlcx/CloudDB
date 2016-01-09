package app_kvServer;

import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.ConsistentHashing;
import ecs.Server;
import common.messages.MessageHandler;
import common.messages.TextMessage;
import metadata.Metadata;

public class ReplicationManager {
	private String name;
    private Metadata metadata;
    private Logger logger;

    private static ReplicationManager replicationManager = null;

	private MessageHandler [] successors = new MessageHandler[2];

	public static ReplicationManager getInstance(){
		return replicationManager;
	}
	
	public static ReplicationManager getInstance(String name, Metadata metadata, Logger logger){
		if(replicationManager == null){
			synchronized(ReplicationManager.class){
				if(replicationManager == null){
					replicationManager = new ReplicationManager(name, metadata, logger);
				}
			}
		}
		return replicationManager;
	}
	
    public ReplicationManager(String name, Metadata metadata, Logger logger){
    	this.name = name;
    	this.metadata = metadata;
    	this.logger = logger;
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
    
    /**
     * get two successors
     * @return two successor server, can be null
     */
    private void updateSuccessors(){
    	Server successor = metadata.getSuccessor(ConsistentHashing.getHashedKey(name));
    	System.out.println(ConsistentHashing.getHashedKey(name));
    	System.out.println(metadata);
    	if(successor != null){
    		try {
				Socket successorSocket= new Socket(successor.ip, Integer.parseInt(successor.port)+20);
				successors[0] = new MessageHandler(successorSocket, logger);
				System.out.println(successor.port);
			} catch (Exception e) {
				// TODO Auto-generated catch block

			}
    		Server sesuccessor = metadata.getSuccessor(successor.hashedkey);
    		if(!sesuccessor.hashedkey.equals(ConsistentHashing.getHashedKey(name))){
    			try{
    				Socket sesuccessorSocket= new Socket(sesuccessor.ip, Integer.parseInt(sesuccessor.port)+20);
    				successors[1] = new MessageHandler(sesuccessorSocket, logger);
    				System.out.println(sesuccessor.port);

    			}catch(Exception e){

    			}
    		}

    	}
    }

    public void update(){
    	updateSuccessors();
    }
}