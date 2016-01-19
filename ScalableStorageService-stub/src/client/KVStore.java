package client;


import java.io.IOException;

import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;


import common.messages.TextMessage;
import client.ClientSocketListener.SocketStatus;
import common.assist.Timestamp;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.Server;
import common.messages.MessageHandler;
import metadata.Metadata;

public class KVStore implements KVCommInterface {	
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private MessageHandler messageHandler;
	
	private volatile boolean running;
	
	private Socket clientSocket; 	
	private Metadata metadata;

	private HashMap<Server, Timestamp> prevs = new HashMap<Server, Timestamp>();
	private HashMap<Server, Integer> sequences = new HashMap<Server, Integer>();
	
 	private String address;
 	private int port;
	
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

	}
	
	public void run() {
		try {
			byte [] msg = messageHandler.receiveMessage();

			TextMessage receivedMessage = new TextMessage(msg);
			receivedMessage = receivedMessage.deserialize();
			metadata = receivedMessage.getMetadata();

			for(ClientSocketListener listener : listeners) {
				listener.handleNewMessage(receivedMessage);
			}
			setRunning(true);
			new Thread(){
				public void run(){
					receive();
				}
			}.start();

		} catch (IOException e) {
			logger.warn("Error while receiving messages.", e);
		}
	}
	private synchronized void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("Tearing down the connection ...");
		if (clientSocket != null) {
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
 	 * Opens the socket for connection to the server.
 	 * @throws IOException 
 	 */
	@Override
	public void connect() throws IOException, UnknownHostException {
		try {
			clientSocket = new Socket(address, port);
			messageHandler = new MessageHandler(clientSocket, logger);
			
			run();
			logger.info("Connection established");

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
	 * put command and send to the responsible server.
	 * If SERVER_NOT_RESPONSIBLE is received, update the metadata, reestablish the
	 * connection with the correct server and retry the put operation.
	 */
	@Override
	public KVMessage put(String key, String value) throws Exception {
		TextMessage sentMessage = new TextMessage();
		sentMessage.setStatusType(StatusType.PUT);
		sentMessage.setKey(key);
		sentMessage.setValue(value);
	
		Server coordinator = metadata.getServerForKey(key);

		if(prevs.get(coordinator) == null){
			prevs.put(coordinator, new Timestamp(3));
		}
		
		if(sequences.get(coordinator) == null){
			sequences.put(coordinator, 0);
		}
		
		sentMessage.setPrev(prevs.get(coordinator));
		sentMessage.setSequence(coordinator.ip+coordinator.port+sequences.get(coordinator));
		sequences.put(coordinator, sequences.get(coordinator)+1);

		
		if(!isCoordinator(address, ""+port, key) && !isReplica(address, ""+port, key) && !isSecondReplica(address, ""+port, key)){
			disconnect();
			this.address = coordinator.ip;
			this.port = Integer.parseInt(coordinator.port);
			connect();								
		}

		messageHandler.sendMessage(sentMessage.serialize().getMsg());

		return null;

	}

	/**
	 * Creates a TextMessage that is then filled with required values for the
	 * get command and send to the responsible server.
	 * If SERVER_NOT_RESPONSIBLE is received, update the metadata, reestablish 
	 * the connection with the correct server and retry the get operation.
	 */
	@Override
	public KVMessage get(String key) throws Exception {
		TextMessage sentMessage = new TextMessage();
		sentMessage.setStatusType(StatusType.GET);
		sentMessage.setKey(key);

		Server coordinator = metadata.getServerForKey(key);
		if(prevs.get(coordinator) == null){
			prevs.put(coordinator, new Timestamp(3));
		}
		
		sentMessage.setPrev(prevs.get(coordinator));
		
		Server correctServer = metadata.getServerForKey(key);
		Server successor = metadata.getSuccessor(correctServer.hashedkey);
		Server sesuccessor = null;
		if(successor != null){
			sesuccessor = metadata.getSuccessor(successor.hashedkey);
		}

		if((!correctServer.ip.equals(this.address) || !correctServer.port.equals("" + this.port)) && (!sesuccessor.ip.equals(this.address) || !sesuccessor.port.equals("" + this.port)) && (!successor.ip.equals(this.address) || !successor.port.equals("" + this.port))){
			disconnect();
			this.address = correctServer.ip;
			this.port = Integer.parseInt(correctServer.port);
			connect();
		}

		
		messageHandler.sendMessage(sentMessage.serialize().getMsg());

		return null;
	}
	
	/**
	 * Looks up the correct server for the provided key in the
	 * updated metadata and establishes a new connection to
	 * this server after closing the old connection.
	 * @param key			The key of the original get/put request 
	 * @throws Exception	
	 */
	private void reestablishConnection(String key) throws Exception{
		Server correctserver = metadata.getServerForKey(key);
		
		if(correctserver != null)
		{
			disconnect();
			this.address = correctserver.ip;
			this.port = Integer.parseInt(correctserver.port);
			connect();
		}
	}
	
	private void receive(){		
		while(running){
			
			byte[] msg;
			try {
				msg = messageHandler.receiveMessage();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				return;
			}
			
			TextMessage receivedMessage = new TextMessage(msg);
			receivedMessage = receivedMessage.deserialize();
			
			if(receivedMessage.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)){
				
				if(receivedMessage.getMetadata() != null){
					this.metadata = receivedMessage.getMetadata();
					logger.warn("Metadata stale! Metadata was updated!");
					String key = receivedMessage.getKey();
					String value = receivedMessage.getValue();
					try {
						reestablishConnection(key);
						if(value.equals("nullll")){
							get(key);
						}else{
							put(key, value);
						}

					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				else{
					logger.error("Metadata stale, but no metadata update was received!");
				}
			}
			else{
				if(receivedMessage.getStatus() != StatusType.SERVER_STOPPED && receivedMessage.getStatus() != StatusType.SERVER_WRITE_LOCK){
					Server coordinator = metadata.getServerForKey(receivedMessage.getKey());
					prevs.get(coordinator).merge(receivedMessage.getPrev());
				}
				
				for(ClientSocketListener listener : listeners) {
					listener.handleNewMessage(receivedMessage);
				}
			}
		}
	}

	private boolean isCoordinator(String ip, String port, String key){
		Server server = metadata.getServerForKey(key);
		return (server.ip).equals(ip) && server.port.equals(port);
	}
	
	private boolean isReplica(String ip, String port, String key){
		Server server = metadata.getServerForKey(key);
		Server successor = metadata.getSuccessor(server.hashedkey);

		return successor!= null &&  (successor.ip.equals(ip) && successor.port.equals(port));
	}
	
	private boolean isSecondReplica(String ip, String port, String key){
		Server server = metadata.getServerForKey(key);
		Server successor = metadata.getSuccessor(server.hashedkey);
		Server sesuccessor = metadata.getSuccessor(successor.hashedkey);

		return successor!= null && (sesuccessor.ip.equals(ip) && sesuccessor.port.equals(port));
		
	}

}