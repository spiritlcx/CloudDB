package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.ServerSocket;

import metadata.Metadata;
import store.StorageManager;

import org.apache.log4j.*;

import app_kvServer.ServerState.State;
import common.messages.KVMessage.StatusType;
import common.messages.MessageHandler;
import common.messages.TextMessage;
import ecs.Server;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	
	private Socket clientSocket;
	private ServerSocket serverSocket;

	private Metadata metadata;

	private MessageHandler messageHandler;
	private StorageManager storageManager;
	
    private ReplicationManager replicationManager;
	
	/**
	 * 
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 * @param keyvalue The cache of the server.
	 * @param cacheSize The cache size of the server cache.
	 * @param strategy Persistence strategy of the server: LRU | LFU | FIFO.
	 * @param persistance Instance of Persictance class, which handles the reading and writing to the storage file.
	 * @param metadata Metadata set of the server.
	 */
	public ClientConnection(Socket clientSocket, ServerSocket serverSocket, Metadata metadata) {
		this.clientSocket = clientSocket;
		this.serverSocket = serverSocket;
		this.isOpen = true;
		this.metadata = metadata;
		
		storageManager = StorageManager.getInstance();
		replicationManager= ReplicationManager.getInstance();
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {		
			messageHandler = new MessageHandler(clientSocket, logger);
			
			TextMessage welcomemsg = new TextMessage("Connection to MSRG Echo server established: " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort());
			
			messageHandler.sendMessage(welcomemsg.getMsg());
			
			while(isOpen) {
				try {
					byte[] latestMsg = messageHandler.receiveMessage();

					TextMessage receivedMessage = new TextMessage(latestMsg);
					receivedMessage = receivedMessage.deserialize();
					
					if(receivedMessage != null){
						try{
							action(receivedMessage);
						}catch(Exception e){
							logger.error(e.getMessage());
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
		}
	}
		
	/**
	 * Performs a certain action depending on the receivedMessage from the client. 
	 * Differentiates messages via the status of the client message: put or get.
	 * It is synchronized since other clients should not change the cache or 
	 * storage simultaneously.
	 * @param receivedMessage The message received from the client
	 */
	private void action(TextMessage receivedMessage){
		switch(receivedMessage.getStatus()){
		case PUT:
			put(receivedMessage.getKey(), receivedMessage.getValue());
			break;
		case GET:
			get(receivedMessage.getKey());
			break;
		default:
			logger.error("No valid status.");
			break;
		}
	}
	
	private void put(String key, String value){
		TextMessage sentMessage = new TextMessage();

		if(KVServer.state.getState() == State.STOP){
			sentMessage.setStatusType(StatusType.SERVER_STOPPED);
		}else if(KVServer.state.getState() == State.LOCK){
			sentMessage.setStatusType(StatusType.SERVER_WRITE_LOCK);			
		}else{
	
			if(isCoordinator("127.0.0.1", ""+serverSocket.getLocalPort(), key)){
				StatusType type = storageManager.put(key, value);
				sentMessage.setStatusType(type);
				replicationManager.replicate(type, key, value);
			}else{
				sentMessage.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
				sentMessage.setMetadata(metadata);
			}
			sentMessage.setKey(key);
			sentMessage.setValue(value);
		}
		
		try {
			messageHandler.sendMessage(sentMessage.serialize().getMsg());
		} 
		catch (IOException e) {
			logger.error("Unable to send response!", e);
		}
	}
	private void get(String key){
		TextMessage sentMessage = new TextMessage();
		String value = null;

		if(KVServer.state.getState() == State.STOP){
			sentMessage.setStatusType(StatusType.SERVER_STOPPED);
		}else{
			
			if(isCoordinator("127.0.0.1", ""+serverSocket.getLocalPort(), key) ||
					isReplica("127.0.0.1", ""+serverSocket.getLocalPort(), key)){
				value = storageManager.get(key);
				if(value == null){
					sentMessage.setStatusType(StatusType.GET_ERROR);
				}else{
					sentMessage.setStatusType(StatusType.GET_SUCCESS);					
				}
			}else{
				sentMessage.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
				sentMessage.setMetadata(metadata);				
			}
		}

		sentMessage.setKey(key);
		sentMessage.setValue(value);

		try {
			messageHandler.sendMessage(sentMessage.serialize().getMsg());
		} catch (IOException e) {
			logger.error("Unable to send response!", e);
		}
	}
	
	private boolean isCoordinator(String ip, String port, String key){
		Server server = metadata.getServerForKey(key);
		return (server.ip).equals(ip) && server.port.equals(port);
	}
	
	private boolean isReplica(String ip, String port, String key){
		Server server = metadata.getServerForKey(key);
		Server successor = metadata.getSuccessor(server.hashedkey);
		Server sesuccessor = metadata.getSuccessor(successor.hashedkey);

		return successor!= null && ((sesuccessor.ip.equals(ip) && sesuccessor.port.equals(port)) || (successor.ip.equals(ip) && successor.port.equals(port)));
	}
}