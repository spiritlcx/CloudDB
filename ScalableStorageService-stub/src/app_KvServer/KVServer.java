package app_KvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;
import strategy.Strategy;
import strategy.StrategyFactory;

public class KVServer extends Thread{
	
	
	private static Logger logger = Logger.getRootLogger();
	
	private int port;
	private int cacheSize;
	private Strategy strategy;
    private ServerSocket serverSocket;
    private boolean running;
    private HashMap<String, String> keyvalue;
    private Persistance persistance;
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
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = StrategyFactory.getStrategy(strategy);
		keyvalue = new HashMap<String, String>();
		this.persistance = new Persistance();
	}
    /**
     * Initializes and starts the server. 
     * Loops until the the server should be closed.
     */
    public void run() {
        
    	running = initializeServer();
        
        if(serverSocket != null) {
	        while(isRunning()){
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
        logger.info("Server stopped.");
    }
    
    private boolean isRunning() {
        return this.running;
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

    private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }
    
    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy =args[2];
				if(strategy.equals("FIFO") || strategy.equals("LRU") || strategy.equals("LFU")){
					new KVServer(port, cacheSize, strategy).start();
				}
				else{
					System.out.println("Error! Invalid argument <strategy>! Must be one of the following: FIFO | LFU | LRU!");
					System.out.println("Usage: Server <port> <cacheSize> <strategy>!");
				}
			}
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
