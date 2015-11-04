package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.*;

import common.messages.KVMessage.StatusType;
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
	private InputStream input;
	private OutputStream output;

	private int cacheSize;
	private Strategy strategy;
	private HashMap<String, String> keyvalue;
	private Persistance persistance;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, HashMap<String, String> keyvalue, int cacheSize, Strategy strategy, Persistance persistance) {
		this.clientSocket = clientSocket;
		this.keyvalue = keyvalue;
		this.isOpen = true;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.persistance = persistance;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			sendMessage(new TextMessage(
					"Connection to MSRG Echo server established: " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort()));
			
			while(isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();
					TextMessage receivedMessage = latestMsg.deserialize();

					if(receivedMessage != null){
						action(receivedMessage);
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
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }
	
	
	private TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");
		return msg;
    }
	
	//action that the server will do after receiving message from client
	private void action(TextMessage receivedMessage){
		TextMessage sentMessage = new TextMessage();
		switch(receivedMessage.getStatus()){
		case PUT:
			if(receivedMessage.getKey() == null){
				logger.error("No valid key value pair.");
				return;
			}
			synchronized(keyvalue){
			    synchronized(persistance){
			    	if(receivedMessage.getValue().equals("")){
			    		if(keyvalue.get(receivedMessage.getKey()) != null){
			    			sentMessage.setValue(keyvalue.get(receivedMessage.getKey()));
			    			keyvalue.remove(receivedMessage.getKey());
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
			    			sendMessage(sentMessage.serialize());
			    		} catch (IOException e) {
			    			logger.error("Unable to send response!", e);
			    		}
			    		return;
			    	}
			    	else{
			    		synchronized(strategy) {
			    			if(keyvalue.size() == cacheSize){
			    				String key = strategy.get();
			    				String value = keyvalue.get(key);
			    				persistance.store(key, value);
			    				keyvalue.remove(key);
			    			}
				
			    			if(keyvalue.get(receivedMessage.getKey()) != null){
			    				keyvalue.remove(receivedMessage.getKey());
			    				sentMessage.setStatusType(StatusType.PUT_UPDATE);
			    			}
			    			else if(persistance.lookup(receivedMessage.getKey()) != null){
			    				String removeMessage = persistance.remove(receivedMessage.getKey());
			    				logger.info(removeMessage);
			    				if(removeMessage.contains("succesfully")){
			    					sentMessage.setStatusType(StatusType.PUT_UPDATE);
			    				}
			    				else{
			    					sentMessage.setStatusType(StatusType.PUT_ERROR);
			    				}
			    			}
			    			else{
			    				sentMessage.setStatusType(StatusType.PUT_SUCCESS);
			    			}

			    			if(sentMessage.getStatus() != StatusType.PUT_ERROR){
			    				keyvalue.put(receivedMessage.getKey(), receivedMessage.getValue());
			    				strategy.add(receivedMessage.getKey());
			    			}
			    			sentMessage.setKey(receivedMessage.getKey());
			    			sentMessage.setValue(receivedMessage.getValue());
			    			try {
			    				sendMessage(sentMessage.serialize());
			    			} 
			    			catch (IOException e) {
			    				logger.error("Unable to send response!", e);
			    			}
			    		}
			    	}
			    }
			}
			break;
		case GET:
			if(receivedMessage.getKey() == null){
				logger.error("not valid key");
				return;
			}
			String value = null;
			if(keyvalue.get(receivedMessage.getKey()) != null){
				value = keyvalue.get(receivedMessage.getKey());
			} else {
				value = persistance.lookup(receivedMessage.getKey());
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
				sendMessage(sentMessage.serialize());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		default:
			logger.error("No valid status.");
			break;
		}
	}
}