package app_KvServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.StatusType;
import common.messages.MessageHandler;
import ecs.ConsistentHashing;
import ecs.Server;
import logger.LogSetup;
import metadata.Metadata;
import strategy.Strategy;
import strategy.StrategyFactory;

public class KVServer{
	private static Logger logger = Logger.getRootLogger();
	
	private int port;
	private int cacheSize;
	private Strategy strategy;
	//serve clients
    private ServerSocket serverSocket;
    //serve movement of data
    private ServerSocket serverMove;
    //serve replications
    private ServerSocket replicaSocket;
    //connect to ECS
    private Socket clientSocket;

	private MessageHandler ecsMsgHandler;
	
    public static AtomicBoolean running = new AtomicBoolean(false);
    private boolean shutdown;
    public static boolean lock;
    private HashMap<String, String> keyvalue;
    private Persistance persistance;
    private Metadata metadata;
        
	private StorageManager storageManager;

	private MessageHandler [] successors = new MessageHandler[2];
	private ReplicationManager replicationManager;
	private FailureDetector failureDetector;
	
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
		try {
			new LogSetup("logs/server.log", Level.ALL);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		shutdown = false;
		running.set(false);
		keyvalue = new HashMap<String, String>();
	}
    /**
     * Initializes and starts the server. 
     * Loops until the the server should be closed.
     */
    public void run(int port) {
    	this.port = port;
    	try {
			initializeServer();
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			return;
		}
    	
		Thread messageReceiver= new Thread(){
			public void run(){
				communicateECS();
			}
		};
		
		messageReceiver.start();

        while(!shutdown){
			try {
				synchronized(running){
					while(!running.get())
						running.wait();
	            	logger.info("start to serve");

				}
				
			}catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				Socket client = serverSocket.accept();
               	ClientConnection connection = 
                		new ClientConnection(client, serverSocket, keyvalue, cacheSize, strategy, persistance, metadata, successors);
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
    
    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public void stopServer(){
        running.set(false);
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
    }
	
	private void communicateECS(){
		KVAdminMessage message = null;
		while(!shutdown){
			try {
				byte [] b =ecsMsgHandler.receiveMessage();
				
				message = new KVAdminMessage(b);
				message = message.deserialize(new String(b, "UTF-8"));
			}catch (IOException e) {
				// TODO Auto-generated catch block
				shutDown();
				logger.error(e);

				return;
			}

			switch(message.getStatusType()){
			case INIT:{
				Metadata meta = message.getMetadata();
				int size = message.getCacheSize();
				String stra = message.getDisplacementStrategy();

				initKVServer(meta, size, stra);

				break;
			}
			case MOVE:{
				String from = message.getFrom();
				String to = message.getTo();
				String ip = message.getIp();
				int port = message.getPort();
				
				try {
					moveData(from, to, ip, port);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			}
			case RECEIVE:
				try {
					receiveData();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			case REMOVE:{
				String from = message.getFrom();
				String to = message.getTo();
				storageManager.removeData(from, to);
				break;
			}
			case SHUTDOWN:
				shutDown();
				break;
			case START:
				start();
				break;
			case STOP:
				stop();
				break;
			case UPDATE:{
				Metadata meta = message.getMetadata();
				setMetadata(meta);
				break;
			}
			case WRITELOCK:
				lock = !lock;
				break;
			default:
				break;			
			}								
		}
	}
    
    private void initializeServer() throws Exception {
    	logger.info("Initialize server ...");
    	
    	try {
    		while(clientSocket == null){
	    		try{
	    			clientSocket = new Socket("127.0.0.1", 40000);
	    		}catch(Exception e){

	    		}
    		}
			ecsMsgHandler = new MessageHandler(clientSocket.getInputStream(),  clientSocket.getOutputStream(), logger);
			
			KVAdminMessage msg = new KVAdminMessage();
			msg.setStatusType(StatusType.RECEIVED);
			msg.setPort(port);

			this.persistance = new Persistance("127.0.0.1", port);
			
			ecsMsgHandler.sendMessage(msg.serialize().getMsg());
						
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
			throw new Exception();
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
    	this.setMetadata(metadata);
    	this.cacheSize = cacheSize;
		this.strategy = StrategyFactory.getStrategy(displacementStrategy);
		shutdown = false;

		try {
			failureDetector = new FailureDetector("127.0.0.1", port);
			for(Server server :metadata.getServers().values()){
				if(server.ip.equals("127.0.0.1") && server.port.equals(""+port))
					continue;
				failureDetector.add(server.ip, Integer.parseInt(server.port));
			}

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			logger.error("failure detector cannot be initialized");
		}
		
		try {
			replicaSocket = new ServerSocket(port+20);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		storageManager = StorageManager.getInstance(keyvalue, metadata, strategy, cacheSize, persistance, logger);
    	replicationManager = ReplicationManager.getInstance(successors, logger);
		
		RepConnection repConnection = new RepConnection(replicaSocket, storageManager, logger);
		Thread thread = new Thread(repConnection);
		thread.start();

    }

    /**
     *Starts the KVServer, all client requests and all ECS requests are 
	 *processed.  
     */

    public void start(){
    	
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

    	
    	synchronized(running){
    		running.set(true);
    		running.notifyAll();
    	}

		updateSuccessors();
		if(!failureDetector.isAlive())
			failureDetector.start();

    	logger.info("The server is started");
    }

    /**
     * Stops the KVServer, all client requests are rejected and only 
		ECS requests are processed.
     */
    
    public void stop(){
    	running.set(false);
    	logger.info("The server is stopped");
    	try {
			serverSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /**
     * Exits the KVServer application. 
     */
    
    public void shutDown(){
    	shutdown = true;
    	try {
			serverSocket.close();
			clientSocket.close();
			replicaSocket.close();
			failureDetector.terminate();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	logger.info("The server is shutdown");
    }

    /**
     *Lock the KVServer for write operations. 
     */
    public void lockWrite(){
    	lock = true;
    	logger.info("The server is locked");
    }
    
    /**
     * Unlock the KVServer for write operations. 
     */
    
    public void unLockWrite(){
    	lock = false;
    	logger.info("The server is unlocked");
    }
    
/*    /**
     * Transfer a subset (range) of the KVServer's data to another 
		KVServer (reallocation before removing this server or adding a 
		new KVServer to the ring); send a notification to the ECS, if data 
		transfer is completed. 
     */

    
    private void moveData(String from, String to, String ip, int port) throws UnknownHostException, IOException{    	

    	KVAdminMessage dataMessage = new KVAdminMessage();
		dataMessage.setStatusType(StatusType.DATA);
		
		Socket moveSender = new Socket(ip, port);
				
		MessageHandler senderHandler = new MessageHandler(moveSender.getInputStream(), moveSender.getOutputStream(), logger);
		
		String data = "";

		int count = 0;
		
		synchronized(keyvalue){
			for(String key : keyvalue.keySet()){
				String hashedkey= ConsistentHashing.getHashedKey(key);
				
				if(to.compareTo(from) < 0){
					if(hashedkey.compareTo(from) > 0 || hashedkey.compareTo(to) < 0){
						count++;
						String value = keyvalue.get(key);
							data += (key + " " + value);
						data += ":";									
					}
					continue;
				}
				
				if(hashedkey.compareTo(from) > 0 && hashedkey.compareTo(to) < 0){
					count++;
					String value = keyvalue.get(key);
						data += (key + " " + value);
					data += ".";
				}
			}
		}

//		synchronized(persistance){
//			persistance.
//		}
		
		dataMessage.setData(data);

		senderHandler.sendMessage(dataMessage.serialize().getMsg());
							
		moveSender.close();

		KVAdminMessage moveFinished = new KVAdminMessage();
		moveFinished.setStatusType(StatusType.MOVEFINISH);
		ecsMsgHandler.sendMessage(moveFinished.serialize().getMsg());
		
    	logger.info(count + " keyvalue pair are transferred");
		
    }
    
    private void removeData(String from, String to){
    	storageManager.removeData(from, to);
    }
    
    private HashMap<String, String> receiveData() throws IOException{
		serverMove = new ServerSocket(port-20);

		KVAdminMessage preparedMessage = new KVAdminMessage();
		preparedMessage.setStatusType(StatusType.PREPARED);
		ecsMsgHandler.sendMessage(preparedMessage.serialize().getMsg());
		
		Socket clientMove = serverMove.accept();

		
		InputStream moveinput = clientMove.getInputStream();
		MessageHandler receiverHandler = new MessageHandler(moveinput, null, logger);
		byte [] datab = receiverHandler.receiveMessage();

		KVAdminMessage receivedData = new KVAdminMessage(datab);
		receivedData = receivedData.deserialize(receivedData.getMsg());
		
		HashMap<String, String> receivedPairs = new HashMap<String, String>();
		
		if(receivedData.getStatusType() == StatusType.DATA){
			String datamsg = receivedData.getData();

			if(datamsg.contains(":")){
				String [] pairs = datamsg.split(":");
	
				for(String pair : pairs){
					String[] kvpair = pair.split(" ");
					if(kvpair.length == 2){
						receivedPairs.put(kvpair[0], kvpair[1]);
						if(keyvalue.size() < cacheSize){
							synchronized(keyvalue){
								keyvalue.put(kvpair[0], kvpair[1]);
							}
						}else{
							persistance.store(kvpair[0], kvpair[1]);
						}
					}
				}
			   	logger.info(pairs.length + " key value pairs are received");
			}else{
			   	logger.info("0 key value pairs are received");
				
			}
		}else{
			logger.error("Format of received data is not correct");
		}

		serverMove.close();
		clientMove.close();
		
		return receivedPairs;
    }
    
    public void removeData(ArrayList<String> toRemove){
		if(lock == true){
			for(String key: toRemove){
				synchronized(keyvalue){
					keyvalue.remove(key);
				}
			}
		}
		logger.info(toRemove.size() + "key vlaue pairs are removed");

    }
    
    /**
     * Update the meta­data repository of this server 
     */
    
    public void update(Metadata metadata){
    	
    	for(Server server : this.metadata.getServers().values()){
    		if(!metadata.getServers().values().contains(server)){
    			failureDetector.remove(server.ip, Integer.parseInt(server.port));
    		}
    	}
    	
    	this.setMetadata(metadata);
    	updateSuccessors();
    	
		logger.info("metadata is updated: " + metadata);
    }
    
    /**
     * get two successors
     * @return two successor server, can be null
     */
    private void updateSuccessors(){
    	Server successor = metadata.getSuccessor(ConsistentHashing.getHashedKey("127.0.0.1" + port));
    	
    	if(successor != null){
    		try {
				Socket successorSocket= new Socket(successor.ip, Integer.parseInt(successor.port)+20);
				successors[0] = new MessageHandler(successorSocket.getInputStream(), successorSocket.getOutputStream(), logger);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
    		Server sesuccessor = metadata.getSuccessor(successor.hashedkey);
    		if(!sesuccessor.hashedkey.equals(ConsistentHashing.getHashedKey("127.0.0.1" + port))){
    			try{
    				Socket sesuccessorSocket= new Socket(sesuccessor.ip, Integer.parseInt(sesuccessor.port)+20);
    				successors[1] = new MessageHandler(sesuccessorSocket.getInputStream(), sesuccessorSocket.getOutputStream(), logger);
    			}catch(Exception e){
    				logger.error(e.getMessage());    				
    			}
    		}

    	}
    }
        
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			KVServer kvserver = new KVServer();
			kvserver.run(Integer.parseInt(args[0]));
//			kvserver.run(50007);

		}catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port> or <cacheSize>! Not a number!");
			System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
			System.exit(1);
		}
    }
	public Metadata getMetadata() {
		return metadata;
	}
	public void setMetadata(Metadata metadata) {
		this.metadata = metadata;
	}    
}