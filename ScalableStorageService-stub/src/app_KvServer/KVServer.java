package app_KvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.StatusType;
import common.messages.MessageHandler;
import ecs.ConsistentHashing;
import logger.LogSetup;
import metadata.Metadata;
import strategy.Strategy;
import strategy.StrategyFactory;

public class KVServer{
	
	
	private static Logger logger = Logger.getRootLogger();

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private int port;
	private int cacheSize;
	private Strategy strategy;
    private ServerSocket serverSocket;
    private ServerSocket serverMove;
    private Socket clientSocket;

    private InputStream input;
	private OutputStream output;
	private MessageHandler messageHandler;
	
    private boolean running;
    private boolean shutdown;
    public static boolean lock;
    private HashMap<String, String> keyvalue;
    private Persistance persistance;
    private Metadata metadata;
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	public KVServer() {
		shutdown = false;
		running = false;
		keyvalue = new HashMap<String, String>();
		this.persistance = new Persistance();
	}
    /**
     * Initializes and starts the server. 
     * Loops until the the server should be closed.
     */
    public void run() {

    	Thread t = new Thread(){
    		public void run(){
    			initializeServer();
    		}
    	};
    	
    	t.start();
    	try {
			t.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.getMessage());
		}
    	
        if(serverSocket != null) {
	        while(!shutdown){
	        	if(running){
		            try {
		                Socket client = serverSocket.accept();  
		                ClientConnection connection = 
		                		new ClientConnection(client, keyvalue, cacheSize, strategy, persistance);
		               (new Thread(connection)).start();
		                
		                logger.info("Connected to " 
		                		+ client.getInetAddress().getHostName() 
		                		+  " on port " + client.getPort());
		            } catch (IOException e) {
		            	logger.error("Error! " +
		            			"Unable to establish connection. \n", e);
		            }
	        	}
	        }
        }
        logger.info("Server stopped.");
    }
    
    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public void stopServer(){
        running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
    }

	
	private void communicateECS(){
		while(!shutdown){
			try {
				byte [] b =messageHandler.receiveMessage();
				
				KVAdminMessage message = new KVAdminMessage(b);
				message = message.deserialize(new String(b, "UTF-8"));
				if(message == null){
					logger.error(message.getMsg() + " is not complete data");
					continue;
				}
				switch(message.getStatusType()){
				case INIT:
					Metadata meta = message.getMetadata();
					int cacheSize = message.getCacheSize();
					String strategy = message.getDisplacementStrategy();
					initKVServer(meta, cacheSize, strategy);
	                System.out.println(cacheSize);

					break;
				case MOVE:
					
					KVAdminMessage dataMessage = new KVAdminMessage();
					dataMessage.setStatusType(StatusType.DATA);
					ConsistentHashing conHashing;
					try {
						conHashing = new ConsistentHashing();
					} catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return;
					}
					
					String from = message.getFrom();
					String to = message.getTo();
					String ip = message.getIp();

					Socket moveSender = new Socket(ip, 30000);
					
					MessageHandler senderHandler = new MessageHandler(moveSender.getInputStream(), moveSender.getOutputStream(), logger);
					
					String data = "";

					ArrayList<String> toRemove = new ArrayList<String>();
					
					synchronized(keyvalue){
						for(String key : keyvalue.keySet()){
							String hashedkey= conHashing.getHashedKey(key);
							System.out.println(hashedkey);
							
							if(to.compareTo(from) < 0){
								if(hashedkey.compareTo(from) > 0 || hashedkey.compareTo(to) < 0){
									toRemove.add(key);
									String value = keyvalue.get(key);
										data += (key + " " + value);
									data += ".";									
								}
								continue;
							}
							
							if(hashedkey.compareTo(from) > 0 && hashedkey.compareTo(to) < 0){
								toRemove.add(key);
								String value = keyvalue.get(key);
									data += (key + " " + value);
								data += ".";
							}
						}
					}

					dataMessage.setData(data);

					senderHandler.sendMessage(dataMessage.serialize().getMsg());
					
					for(String key: toRemove){
						synchronized(keyvalue){
							keyvalue.remove(key);
						}
					}
					break;
				case RECEIVE:
					serverMove = new ServerSocket(30000);
					Socket clientMove = serverMove.accept();
					
					
					InputStream moveinput = clientMove.getInputStream();
					MessageHandler receiverHandler = new MessageHandler(moveinput, null, logger);
					byte [] datab = receiverHandler.receiveMessage();

					String datamsg = new String(datab, "UTF-8");

					System.out.println("ssss");
					
					String [] pairs = datamsg.split(".");
					
					for(String pair : pairs){
						String[] kvpair = pair.split(" ");
						if(kvpair.length == 2){
							synchronized(keyvalue){
								keyvalue.put(kvpair[0], kvpair[1]);
							}
						}
					}
					
					serverMove.close();
					clientMove.close();
					moveinput.close();
					
					break;
				case SHUTDOWN:
					shutDown();
					break;
				case START:
					start();
					break;
				case STOP:
					stop();
					break;
				case UPDATE:
					meta = message.getMetadata();
					break;
				default:
					break;
				
				}
								
//				System.out.println(msg);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e);
				shutdown = true;
				return;
			}
		}
	}
    
    private void initializeServer() {
    	logger.info("Initialize server ...");
    	
    	try {
			clientSocket = new Socket("127.0.0.1", 40000);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			messageHandler = new MessageHandler(input, output, logger);
			
			port = 50003;

			KVAdminMessage msg = new KVAdminMessage();
			msg.setStatusType(StatusType.RECEIVED);			
			msg.setPort(port);
			
			messageHandler.sendMessage(msg.serialize().getMsg());
			
			new Thread(){
				public void run(){
					communicateECS();
				}
			}.start();
			
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.getMessage());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.getMessage());
		}
    	
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
        } catch (Exception e){
        	logger.error(e.getMessage());
        }
    }
    
    /**
     * Initialize the KVServer with the meta­data, it’s local cache size, 
		and the cache displacement strategy, and block it for client 
		requests, i.e., all client requests are rejected with an 
		SERVER_STOPPED error message; ECS requests have to be 
		processed. 
     */
    
    public void initKVServer(Metadata metadata, int cacheSize, String displacementStrategy){
    	this.metadata = metadata;
    	this.cacheSize = cacheSize;
		this.strategy = StrategyFactory.getStrategy(displacementStrategy);
		shutdown = false;
    }

    /**
     *Starts the KVServer, all client requests and all ECS requests are 
	 *processed.  
     */
    
    public void start(){
    	running = true;
    }

    /**
     * Stops the KVServer, all client requests are rejected and only 
		ECS requests are processed.
     */
    
    public void stop(){
    	running = false;
    }
    
    /**
     * Exits the KVServer application. 
     */
    
    public void shutDown(){
    	shutdown = true;
    }

    /**
     *Lock the KVServer for write operations. 
     */
    public void lockWrite(){
    	lock = true;
    }
    
    /**
     * Unlock the KVServer for write operations. 
     */
    
    public void unLockWrite(){
    	lock = false;
    }
    
    /**
     * Transfer a subset (range) of the KVServer’s data to another 
		KVServer (reallocation before removing this server or adding a 
		new KVServer to the ring); send a notification to the ECS, if data 
		transfer is completed. 
     */
    
//    public void moveData(range, server){
//    	
//    	
//    }
//    
//    /**
//     * Update the meta­data repository of this server 
//     */
//    
    public void update(Metadata metadata){
    	this.metadata = metadata;
    }
    
    
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/server.log", Level.ALL);
			KVServer kvserver = new KVServer();
			kvserver.run();
//			if(args.length != 3) {
//				System.out.println("Error! Invalid number of arguments!");
//				System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
//			} else {
//				int port = Integer.parseInt(args[0]);
//				int cacheSize = Integer.parseInt(args[1]);
//				String strategy =args[2];
//				if(strategy.equals("FIFO") || strategy.equals("LRU") || strategy.equals("LFU")){
//					new KVServer(port, cacheSize, strategy).run();
//				}
//				else{
//					System.out.println("Error! Invalid argument <strategy>! Must be one of the following: FIFO | LFU | LRU!");
//					System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
//				}
//			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port> or <cacheSize>! Not a number!");
			System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
			System.exit(1);
		}
    }    
}
