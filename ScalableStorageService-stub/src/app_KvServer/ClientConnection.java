package app_KvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.HashMap;

import metadata.Metadata;

import org.apache.log4j.*;

import common.messages.KVMessage.StatusType;
import common.messages.MessageHandler;
import common.messages.TextMessage;
import strategy.Strategy;


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
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private ServerSocket serverSocket;
	private InputStream input;
	private OutputStream output;

	private int cacheSize;
	private Strategy strategy;
	private HashMap<String, String> keyvalue;
	private Persistance persistance;
	private Metadata metadata;

	private MessageHandler messageHandler;
	
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
	public ClientConnection(Socket clientSocket, ServerSocket serverSocket, HashMap<String, String> keyvalue, int cacheSize, Strategy strategy, Persistance persistance, Metadata metadata) {
		this.clientSocket = clientSocket;
		this.serverSocket = serverSocket;
		this.keyvalue = keyvalue;
		this.isOpen = true;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.persistance = persistance;
		this.metadata = metadata;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			messageHandler = new MessageHandler(input, output, logger);
			
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
							if(KVServer.running.get() == true)
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
					input.close();
					output.close();
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
			if(KVServer.lock)
				return;
			put(receivedMessage);
			break;
		case GET:
			get(receivedMessage);
			break;
		default:
			logger.error("No valid status.");
			break;
		}
	}
	
	private void put(TextMessage receivedMessage){
		TextMessage sentMessage = new TextMessage();

		if(receivedMessage.getKey() == null){
			logger.error("No valid key value pair.");
			return;
		}
		
		String[] server = metadata.getServer(receivedMessage.getKey());
					
		if(server != null && (server[0]).equals("127.0.0.1") && server[1].equals("" + serverSocket.getLocalPort())){
			if(receivedMessage.getValue().equals("null")){
				if(keyvalue.get(receivedMessage.getKey()) != null){
					sentMessage.setValue(keyvalue.get(receivedMessage.getKey()));
					synchronized(keyvalue){
						keyvalue.remove(receivedMessage.getKey());
					}
					synchronized(strategy){
						strategy.remove(receivedMessage.getKey());
					}
					String keyToLoad = null;
					if((keyToLoad = persistance.read()) != null){
						String valueToLoad = persistance.lookup(keyToLoad);
						synchronized(keyvalue){
							keyvalue.put(keyToLoad, valueToLoad);
						}
						persistance.remove(keyToLoad);
						synchronized(strategy){
							strategy.add(keyToLoad);
						}
					}
				
					sentMessage.setStatusType(StatusType.DELETE_SUCCESS);
				} 
				else if(persistance.lookup(receivedMessage.getKey()) != null){
					String removeMessage = persistance.remove(receivedMessage.getKey());
					logger.info(removeMessage);
					if(removeMessage.contains("succesfully")){
						sentMessage.setStatusType(StatusType.DELETE_SUCCESS);
					}
					else{
						sentMessage.setStatusType(StatusType.DELETE_ERROR);
					}
				}
				else{
					sentMessage.setStatusType(StatusType.DELETE_ERROR);						
				}
		    		
				sentMessage.setKey(receivedMessage.getKey());
				
				try {
					messageHandler.sendMessage(sentMessage.serialize().getMsg());
				} catch (IOException e) {
					logger.error("Unable to send response!", e);
				}
				return;
			}
			else{
			
				if(keyvalue.get(receivedMessage.getKey()) != null){
					synchronized(keyvalue){
						keyvalue.put(receivedMessage.getKey(), receivedMessage.getValue());
					}
					synchronized(strategy){
						strategy.update(receivedMessage.getKey());
					}
					sentMessage.setStatusType(StatusType.PUT_UPDATE);
				}else{
					if(persistance.lookup(receivedMessage.getKey()) != null){
						String removeMessage = persistance.remove(receivedMessage.getKey());
						logger.info(removeMessage);
					
						//remove a key in keyvalue and put the new key						
						if(removeMessage.contains("succesfully")){
							String key = strategy.get();
							if(key != null){
								String value = keyvalue.get(key);
								persistance.store(key, value);
								synchronized(strategy){
									strategy.remove(key);
								}
								keyvalue.remove(key);
							}

							keyvalue.put(receivedMessage.getKey(), receivedMessage.getValue());
							sentMessage.setStatusType(StatusType.PUT_UPDATE);

						}
						else{
							sentMessage.setStatusType(StatusType.PUT_ERROR);
						}
					}else{
						if(keyvalue.size() >= cacheSize){
							String key = strategy.get();
							String value = keyvalue.get(key);
							persistance.store(key, value);
							synchronized(keyvalue){
								keyvalue.remove(key);
								keyvalue.put(receivedMessage.getKey(), receivedMessage.getValue());
							}
							synchronized(strategy){
								strategy.remove(key);
								strategy.add(receivedMessage.getKey());
							}
						}else{
							synchronized(keyvalue){
								keyvalue.put(receivedMessage.getKey(), receivedMessage.getValue());
							}
							synchronized(strategy){
								strategy.add(receivedMessage.getKey());
							}

						}
						sentMessage.setStatusType(StatusType.PUT_SUCCESS);
					}
				}
	 	
				sentMessage.setKey(receivedMessage.getKey());
				sentMessage.setValue(receivedMessage.getValue());
				try {
					messageHandler.sendMessage(sentMessage.serialize().getMsg());
				} 
				catch (IOException e) {
					logger.error("Unable to send response!", e);
				}
			}
		}
		else{
			sentMessage.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
			sentMessage.setKey(receivedMessage.getKey());
			sentMessage.setValue(receivedMessage.getValue());
			sentMessage.setMetadata(metadata);

			try {
				messageHandler.sendMessage(sentMessage.serialize().getMsg());
			} 
			catch (IOException e) {
				logger.error("Unable to send response!", e);
			}
		}

	}
	private void get(TextMessage receivedMessage){
		TextMessage sentMessage = new TextMessage();

		if(receivedMessage.getKey() == null){
			logger.error("not valid key");
			return;
		}
		
		String [] server = metadata.getServer(receivedMessage.getKey());
		
		if(server != null && server[0].equals("127.0.0.1") && server[1].equals("" + clientSocket.getLocalPort())){
			String value = null;
			if(keyvalue.get(receivedMessage.getKey()) != null){
				value = keyvalue.get(receivedMessage.getKey());
			} else {
				value = persistance.lookup(receivedMessage.getKey());
				if(value != null){
					persistance.remove(receivedMessage.getKey());
					String keyToRemove = strategy.get();
					if(keyToRemove != null){
						String valueToRemove = keyvalue.get(keyToRemove);
						persistance.store(keyToRemove, valueToRemove);
						synchronized(keyvalue){
							keyvalue.remove(keyToRemove);
							}
						synchronized(strategy){
							strategy.remove(keyToRemove);
						}
					}
					keyvalue.put(receivedMessage.getKey(), value);
					strategy.add(receivedMessage.getKey());
				}
			}
			if(value != null){
				sentMessage.setStatusType(StatusType.GET_SUCCESS);
				sentMessage.setKey(receivedMessage.getKey());
				sentMessage.setValue(value);
			} else {
				sentMessage.setStatusType(StatusType.GET_ERROR);
				sentMessage.setKey(receivedMessage.getKey());				
			}
			try {
				messageHandler.sendMessage(sentMessage.serialize().getMsg());
			} catch (IOException e) {
				logger.error("Error while getting value!", e);
			}
		}
		else{
			sentMessage.setStatusType(StatusType.SERVER_NOT_RESPONSIBLE);
			sentMessage.setKey(receivedMessage.getKey());
			sentMessage.setMetadata(metadata);
			
			try {
				messageHandler.sendMessage(sentMessage.serialize().getMsg());
			} 
			catch (IOException e) {
				logger.error("Unable to send response!", e);
			}
		}

	}
}