package client;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;


import common.messages.TextMessage;
import client.ClientSocketListener.SocketStatus;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class KVStore extends Thread implements KVCommInterface {	
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;
	
	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;
 	
 	private String address;
 	private int port;
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public KVStore(String address, int port){
		
		this.address = address;
		this.port = port;
		
		listeners = new HashSet<ClientSocketListener>();
		setRunning(true);
		logger.info("Connection established");

	}
	
	public void run() {
		try {
			TextMessage receivedMessage = receiveMessage();
			for(ClientSocketListener listener : listeners) {
				listener.handleNewMessage(receivedMessage);
			}
			
		} catch (IOException e) {
			logger.warn("Error while receiving messages.", e);
		}
	}
	private synchronized void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("Tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("Connection closed!");
		}
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public void setRunning(boolean run) {
		running = run;
	}
	
	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
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
		logger.info("Send message:\t '" + msg.getMsg() + "'");
    }
	
	/**
	 * Receives message from the server and converts it to a TextMessage.
	 * @return A TextMessage representing server response.
	 * @throws IOException Some exception while reading the input stream occured
	 */
	private TextMessage receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();
		if(read == -1)
			return null;
		
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
			
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
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
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
    }
 	/**
 	 * Opens the socket for connection to the server.
 	 * @throws IOException 
 	 */
	@Override
	public void connect() throws IOException, UnknownHostException {
		try {
			clientSocket = new Socket(address, port);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw new UnknownHostException();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw new IOException();
		}
	
	}
/**
 * Disconnects the client from the server and sets the SocketStatus to
 * DISCONNECTED.
 */
	@Override
	public void disconnect() {
		logger.info("Trying to close connection ...");
		
		try {
			tearDownConnection();
			for(ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * put command and sends to the server.
	 */
	@Override
	public KVMessage put(String key, String value) throws Exception {
		TextMessage sentMessage = new TextMessage();
		sentMessage.setStatusType(StatusType.PUT);
		sentMessage.setKey(key);
		sentMessage.setValue(value);

		sendMessage(sentMessage.serialize());

		TextMessage receivedMessage = receiveMessage();

		if(receivedMessage == null){
			running = false;
			return null;
		}
		
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(receivedMessage);
		}

		return (KVMessage)receivedMessage.deserialize();
	
	}

	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * get command and sends to the server.
	 */
	@Override
	public KVMessage get(String key) throws Exception {
		TextMessage sentMessage = new TextMessage();
		sentMessage.setStatusType(StatusType.GET);
		sentMessage.setKey(key);
		
		sendMessage(sentMessage.serialize());
		
		TextMessage receivedMessage = receiveMessage();

		if(receivedMessage == null){
			running = false;
			return null;
		}
		
		for(ClientSocketListener listener : listeners) {
			listener.handleNewMessage(receivedMessage);
		}

		return (KVMessage)receivedMessage.deserialize();
	}
	
}