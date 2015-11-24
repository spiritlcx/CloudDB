package app_KvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.TextMessage;
import common.messages.KVAdminMessage.StatusType;
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
    private Socket clientSocket;

    private InputStream input;
	private OutputStream output;

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
		                System.out.println(cacheSize);
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

	public void sendMessage(KVAdminMessage msg) throws IOException {
		String ms = msg.serialize();
		byte[] msgBytes = ms.getBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.serialize() +"'");
    }


	private String receiveMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();

		if(read == -1)
			throw new IOException();
		
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
		return new String(msgBytes, "UTF-8");
    }
	
	private void communicateECS(){
		while(!shutdown){
			try {
				String msg = receiveMessage();
//				byte [] b = new byte[512];
//				input.read(b);
//				String msg = new String(b, "UTF-8");
				System.out.println(msg);
				KVAdminMessage message = new KVAdminMessage();
				message = message.deserialize(msg);
				if(message == null){
					logger.error(msg + " is not complete data");
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
									
			port = 50000;

			KVAdminMessage msg = new KVAdminMessage();
			msg.setStatusType(StatusType.RECEIVED);			
			msg.setPort(port);
			
			sendMessage(msg);
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
