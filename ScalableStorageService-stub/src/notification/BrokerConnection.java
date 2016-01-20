package notification;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import org.apache.log4j.Logger;

import common.messages.KVBrokerMessage;
import common.messages.MessageHandler;

/**
 * This class handles the connection with an individual subscriber or publisher.
 * Note: in the publisher/subscriber architecture, an entity can have the role of
 * a subscriber as well as a publisher. It's therefore natural to handle
 * both connection types with the same class, although they are technically separated
 * in the current project.
 */

public class BrokerConnection implements Runnable {
	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	
	private Socket clientSocket;
	private MessageHandler messageHandler;
	private MessageBroker parent;

	List<KVBrokerMessage> messageQueue;
		
	/**
	 * 
	 * Constructs a new PubSubConnection object for a given TCP socket.
	 * @param clientSocket 	The Socket object for the client connection.
	 * @param serverSocket 	The ServerSocket of the BrokerService.
	 * @param cacheSize 	Reference to the BrokerService creating the instance of PubSubConnection.
	 */
	public BrokerConnection(Socket clientSocket, MessageBroker parent) {
		this.clientSocket = clientSocket;
		this.parent = parent;
		this.isOpen = true;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {		
			messageHandler = new MessageHandler(clientSocket, logger);
			
			while(isOpen) {
				try {
					byte[] latestMsg = messageHandler.receiveMessage();

					KVBrokerMessage receivedMessage = new KVBrokerMessage(latestMsg);
					receivedMessage = receivedMessage.deserialize();
					
					if(receivedMessage != null){
						try{
							action(receivedMessage);
						}catch(Exception e){
							logger.error(e.getMessage());
						}
					}
					
					if(!messageQueue.isEmpty()){
						try {
							messageHandler.sendMessage(messageQueue.get(0).serialize().getMsg());
						} 
						catch (IOException e) {
							logger.error("Unable to send enqueued response!", e);
						}
					}
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/
					if(!clientSocket.getInetAddress().isReachable(5000)){
						isOpen = false;
						logger.info("Client " + clientSocket.getInetAddress() + " has been disconnected.");
					}
				} 
				catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
			//Finally, delete subscriber from the system
			parent.removeSubscriber(this);
		}
	}
		
	/**
	 * Performs a certain action depending on the receivedMessage from the client. 
	 * Differentiates messages via the status of the client message: put or get.
	 * It is synchronized since other clients should not change the cache or 
	 * storage simultaneously.
	 * @param receivedMessage The message received from the client
	 */
	private void action(KVBrokerMessage receivedMessage){
		switch(receivedMessage.getStatus()){
		case SUBSCRIBE_CHANGE:
			parent.addChangeSubscription(receivedMessage.getKey(), this);
			break;
		case SUBSCRIBE_DELETE:
			parent.addDeleteSubscription(receivedMessage.getKey(), this);
			break;
		case UNSUBSCRIBE_CHANGE:
			parent.removeChangeSubscription(receivedMessage.getKey(), this);
			break;
		case UNSUBSCRIBE_DELETE:
			parent.removeDeleteSubscription(receivedMessage.getKey(), this);
			break;
		case SUBSCRIBTION_UPDATE:
			parent.updateChangeSubscribers(receivedMessage.getKey(), receivedMessage.getValue());
			break;
		case SUBSCRIBTION_DELETE:
			parent.updateDeleteSubscribers(receivedMessage.getKey(), receivedMessage.getValue());
			break;
		default:
			logger.error("No valid status.");
			break;
		}
	}
	
	public synchronized void enqueueMessage(KVBrokerMessage message)
	{
		if(message != null){
			messageQueue.add(message);
		}
	}
}
