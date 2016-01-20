package notification;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import logger.LogSetup;
import notification.BrokerState.State;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.*;
import common.messages.KVAdminMessage.StatusType;

import client.KVStore;

/**
 * The BrokerService class is supposed to be run on a separate server. It works
 * independently of the StorageServers and the ECS, although it is started by the ECS
 * and is connected to the StorageServers.
 * 
 * Oriented on the Publish-Subscribe-Principle, the StorageServers publish changes to their
 * data to the BrokerService. The service then parses its registered subscribers and
 * forwards the information to selected clients.
 * 
 * Since the subscription system is independent of the KVServers, a failure of a server
 * does not prevent the subscription messages from reaching clients.
 * Also, a rearrangement of the system structure does not lead to failure of the 
 * BrokerService.
 * 
 */
public class MessageBroker {

	private SubscriberData changeSubscriberData;
	private SubscriberData deleteSubscriberData;
	private static Logger logger = Logger.getRootLogger();
	
	private ServerSocket serverSocket;
	private int port;
	private KVStore keyVerifier;
	
	private Socket clientSocket;
	private MessageHandler ecsMsgHandler;
	
	public static volatile BrokerState state = new BrokerState();
	
	public MessageBroker(String ip, int port){
		changeSubscriberData = new SubscriberData();
		deleteSubscriberData = new SubscriberData();
		keyVerifier = new KVStore(ip, port);
		
		try {
			new LogSetup("logs/broker.log", Level.ALL);
		} catch (IOException e) {
			logger.error("Error!", e);
		}
	}
	
	public void run(int port){
		this.port = port;
		try {
			initializeBroker();
		} catch (Exception e2) {
			
			return;
		}
    	
		Thread messageReceiver= new Thread(){
			public void run(){
				communicateECS();
			}
		};
		
		messageReceiver.start();

        while(state.getState() != State.SHUTDOWN){
			try {
				synchronized(state){
					while(state.getState() != State.RUNNING)
						state.wait();
	            	logger.info("start to serve");
				}
				
			}catch (InterruptedException e1) {
				
				logger.error("Error!", e1);
			}
			try {
				Socket client = serverSocket.accept();
				BrokerConnection connection = 
                		new BrokerConnection(client, this);
               (new Thread(connection)).start();
                
                logger.info("Connected to " 
                		+ client.getInetAddress().getHostName() 
                		+  " on port " + client.getPort());
            } catch (IOException e) {
            	logger.error("Error! " +
            			"Unable to establish connection. \n", e);
            }
        }
        System.out.println("not running");
	}
	
	public synchronized KVBrokerMessage addChangeSubscription(String key, BrokerConnection client){
		if(testKey(key)){
			if(changeSubscriberData.addSubscription(key, client)){
				return new KVBrokerMessage("{StatusType: SUBSCRIBE_CHANGE_CONFIRM, key:" + key +"}");
			}
			return new KVBrokerMessage("{StatusType: SUBSCRIBTION_ALREADY_EXISTS, key:" + key +"}");
		}
		return new KVBrokerMessage("{StatusType: INVALID_SUBSCRIPTION_TARGET, key:" + key +"}");
	}
	
	public synchronized KVBrokerMessage addDeleteSubscription(String key, BrokerConnection client){
		if(testKey(key)){
			if(deleteSubscriberData.addSubscription(key, client)){
				return new KVBrokerMessage("{StatusType: SUBSCRIBE_DELETE_CONFIRM, key:" + key +"}");
			}
			return new KVBrokerMessage("{StatusType: SUBSCRIBTION_ALREADY_EXISTS, key:" + key +"}");
		}
		return new KVBrokerMessage("{StatusType: INVALID_SUBSCRIPTION_TARGET, key:" + key +"}");
	}
	
	public synchronized KVBrokerMessage removeChangeSubscription(String key, BrokerConnection client){
		if(changeSubscriberData.removeSubscription(key, client)){
			return new KVBrokerMessage("{StatusType: UNSUBSCRIBE_CHANGE_CONFIRM, key:" + key +"}");
		}
		return new KVBrokerMessage("{StatusType: SUBSCRIBTION_DOES_NOT_EXIST, key:" + key +"}");
	}
	
	public synchronized KVBrokerMessage removeDeleteSubscription(String key, BrokerConnection client){
		if(deleteSubscriberData.removeSubscription(key, client)){
			return new KVBrokerMessage("{StatusType: UNSUBSCRIBE_DELETE_CONFIRM, key:" + key +"}");
		}
		return new KVBrokerMessage("{StatusType: SUBSCRIBTION_DOES_NOT_EXIST, key:" + key +"}");
	}
	
	public void updateChangeSubscribers(String key, String value){
		List<BrokerConnection> subscribers = changeSubscriberData.getSubscribers(key);
		
		for(BrokerConnection subscriber: subscribers){
			subscriber.enqueueMessage(new KVBrokerMessage("{StatusType: SUBSCRIBE_DELETE_CONFIRM, key:" + key + ", value:" + value +"}"));
		}
	}
	
	public void updateDeleteSubscribers(String key, String value){
		List<BrokerConnection> subscribers = deleteSubscriberData.getSubscribers(key);
		
		for(BrokerConnection subscriber: subscribers){
			subscriber.enqueueMessage(new KVBrokerMessage("{StatusType: SUBSCRIBE_DELETE_CONFIRM, key:" + key + ", value:" + value +"}"));
		}
	}
	
	private boolean testKey(String key){
		
		TextMessage receivedMessage = null;
		try {
			receivedMessage = (TextMessage)keyVerifier.get(key);
		} catch (Exception e) {
			logger.error("Error!", e);
		}
		
		if(receivedMessage != null && receivedMessage.deserialize().getStatus().equals(KVMessage.StatusType.GET_SUCCESS)){
			return true;
		}
		return false;
	}
	
	public synchronized boolean removeSubscriber(BrokerConnection subscriber)
	{
		return (changeSubscriberData.removeSubscriber(subscriber) & deleteSubscriberData.removeSubscriber(subscriber));
	}
	
	private void communicateECS(){
		KVAdminMessage message = null;
		while(state.getState() != State.SHUTDOWN){
			try {
				byte [] b =ecsMsgHandler.receiveMessage();
				
				message = new KVAdminMessage(b);
				message = message.deserialize(new String(b, "UTF-8"));
			}catch (IOException e) {
				
				state.setState(State.SHUTDOWN);
		    	try {
					serverSocket.close();
					clientSocket.close();
				} catch (IOException e2) {
					logger.error("Error!", e2);
				}
		    	logger.error("The broker service is shutdown.", e);

				return;
			}

			switch(message.getStatusType()){
			case SHUTDOWN:
				state.setState(State.SHUTDOWN);
		    	try {
					serverSocket.close();
					clientSocket.close();
				} catch (IOException e) {
					logger.error("Error!", e);
				}
		    	logger.info("The broker service is shutdown");
				break;
			case START:
				try {
		            serverSocket = new ServerSocket(port);
		            logger.info("Server listening on port: " 
		            		+ serverSocket.getLocalPort());
		        } catch (IOException e) {
		        	logger.error("Error! Cannot open server socket:");
		            if(e instanceof BindException){
		            	logger.error("Port " + port + " is already bound!");
		            }
		        }
				synchronized(state){
					state.setState(State.RUNNING);
		    		state.notifyAll();
		    	}
				logger.info("The broker service is started");
				break;
			case STOP:
				state.setState(State.STOP);
		    	logger.info("The broker service is stopped");
				break;
			default:
				break;
			}								
		}
	}
	
	private void initializeBroker() throws Exception {
    	logger.info("Initialize server ...");
    	
    	try {
    		while(clientSocket == null){
	    		try{
	    			clientSocket = new Socket("127.0.0.1", 40000);
	    		}catch(Exception e){

	    		}
    		}
			ecsMsgHandler = new MessageHandler(clientSocket, logger);
			
			KVAdminMessage msg = new KVAdminMessage();
			msg.setStatusType(StatusType.RECEIVED);
			msg.setPort(port);
			
			ecsMsgHandler.sendMessage(msg.serialize().getMsg());
						
		} catch (Exception e) {
			
			logger.error(e.getMessage());
			throw new Exception();
		}
    }
	
	/**
	 * @param args	<StorageServerIP> <StorageServerPort> <MessageBrokerPort>
	 */
	public static void main(String[] args) {
		try{
			MessageBroker brokerService = new MessageBroker(args[0], Integer.parseInt(args[1]));
			brokerService.run(Integer.parseInt(args[2]));
		}
		catch(Exception e){
			System.out.println("Wrong number of callup parameters or port not a number! Parameters: <StorageServerIP> <StorageServerPort> <MessageBrokerPort>.");
		}
		
	}

}
	
	class BrokerState{
		enum State {
			RUNNING,
			SHUTDOWN,
			LOCK,
			STOP
		}
		private State state;
		public BrokerState(){
			state = State.STOP;
		}
		public void setState(State state){
			this.state = state;
		}
		public State getState(){
			return this.state;
		}
	}	
