package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.StatusType;
import logger.LogSetup;
import metadata.Metadata;

public class ECS {
	
	private Logger logger = Logger.getRootLogger();
	private Metadata metadata = new Metadata();
	private static String start = "00000000000000000000000000000000";
	private static String end = "ffffffffffffffffffffffffffffffff";

	private ServerSocket ecsServer;
	boolean running;
	
	private TreeMap<String, Socket> hashservers = new TreeMap<String, Socket>();
	private HashMap<String, ServerConnection> hashthreads = new HashMap<String, ServerConnection>();
	private ConsistentHashing conHashing;

	private ArrayList<Server> workingservers = new ArrayList<Server>();
	private ArrayList<Server> idleservers = new ArrayList<Server>();
	
	public ECS(){
		try {
			running = true;
			conHashing = new ConsistentHashing();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*Randomly choose <numberOfNodes> servers from the available 
		machines and start the KVServer by issuing a SSH call to the 
		respective machine. This call launches the server with the 
		specified cache size and displacement strategy. You can 
		assume that the KVServer.jar is located in the same directory as 
		the ECS. All servers are initialized with the meta­data and 
		remain in state stopped.
	 */
	public void initService (int numberOfNodes, int cacheSize, String displacementStrategy){
		File file = new File("ecs.config");
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			try {
				while((line = reader.readLine()) != null){
					String [] node = line.split(" ");
					if(node != null && node.length == 3){
						Server server = new Server();
						server.ip = node[1];
						server.port = node[2];

						idleservers.add(server);
					}
				}
				reader.close();

				if(idleservers.size() == 0){
					logger.error("No server available");
					return;
				}
				
				if(idleservers.size() < numberOfNodes){
					logger.error("You can start at most " + idleservers.size() + " servers");
					return;
				}

				Random random = new Random();
				for(int i = 0; i < numberOfNodes; i++){
					int server = random.nextInt(idleservers.size());
					conHashing.add(idleservers.get(server));
					idleservers.remove(server);
				}
				
				workingservers = conHashing.distribute();

				//After receiving servers with hashed keys, it will know how to map keys to
				//each server with the range (from, to) in each server, and store the information
				//in metadata which will be used by both client and server
				
				for(int i = 0; i < workingservers.size(); i++){
					if(workingservers.size() == 1){
						workingservers.get(i).from = start;
						workingservers.get(i).to = end;
						metadata.add(workingservers.get(i));
						break;
					}
					if(i == 0){
						workingservers.get(i).from = workingservers.get(workingservers.size() - 1).hashedkey;
						workingservers.get(i).to = workingservers.get(i).hashedkey;
					}
					else{
						workingservers.get(i).from = workingservers.get(i-1).hashedkey;
						workingservers.get(i).to = workingservers.get(i).hashedkey;
					}
					metadata.add(workingservers.get(i));
				}

				for(Server server : workingservers){
					System.out.println(server.port);
					System.out.println(server.from + " to " + server.to);
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}
		
	/* Starts the storage service by calling start() on all KVServer 
		instances that participate in the service. 
	 */
	public void start(){
		for(String key : hashthreads.keySet()){
			ServerConnection connection = hashthreads.get(key);
			connection.startServer();
		}
	}
	
	/* Stops the service; all participating KVServers are stopped for 
		processing client requests but the processes remain running. 
	 */
	
	public void stop(){
		for(String key : hashthreads.keySet()){
			ServerConnection connection = hashthreads.get(key);
			connection.stopServer();
		}
	}
	
	
	/* Stops all server instances and exits the remote processes. 
	 */
	public void shutDown(){
		for(String key : hashthreads.keySet()){
			ServerConnection connection = hashthreads.get(key);
			connection.shutDown();
		}
	}

	/*
	 * stop the ECS itself
	 */
	public void stopRunning(){
		running = false;
	}
	
	/*Create a new KVServer with the specified cache size and 
	 * displacement strategy and add it to the storage service at an 
	 * arbitrary position. 
	 */
	public void addNode(int newcacheSize, String displacementStrategy){
		//If there are idle servers in the repository, randomly pick one of 
		//them and send an SSH call to invoke the KVServer process.
		
		if(idleservers.size() != 0){
			Random random = new Random();
			Server newworkingserver = idleservers.get(random.nextInt(idleservers.size()));

			//Recalculate and update the meta­data of the storage service
			newworkingserver.hashedkey = conHashing.getHashedKey(newworkingserver.ip, Integer.parseInt(newworkingserver.port));
			workingservers.add(newworkingserver);
			idleservers.remove(newworkingserver);

			Server successor = metadata.putServer(newworkingserver);

			Socket kvserver;
			try {
				kvserver = ecsServer.accept();
				ServerConnection connection = new ServerConnection(this, kvserver.getInputStream(), kvserver.getOutputStream(),newcacheSize, displacementStrategy, metadata, logger);
				hashthreads.put(newworkingserver.hashedkey, connection);
				hashservers.put(newworkingserver.hashedkey, kvserver);

				connection.start();
				connection.receiveData();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}

			ServerConnection suserver = hashthreads.get(successor.hashedkey);
			
			suserver.setWriteLock();
			suserver.moveData(newworkingserver.from, newworkingserver.to, newworkingserver.ip);
									
			KVAdminMessage moveFinished = new KVAdminMessage(suserver.getInput());
			moveFinished = moveFinished.deserialize(moveFinished.getMsg());
			if(moveFinished.getStatusType() == StatusType.MOVEFINISH){
				for(ServerConnection connection : hashthreads.values()){
					connection.update(metadata);
				}
			}

			suserver.releaseWriteLock();
		}
	}

	/* Remove a node from the storage service at an arbitrary position. 
	 */
	public void removeNode(){
		
		
	}
	
	public static void main(String [] args){
		try {
			
			new LogSetup("logs/ecs.log", Level.ALL);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		final ECS ecs = new ECS();
		new Thread(){
			public void run(){
				ecs.startEcs(40000, 2, 10, "FIFO");
			}
		}.start();
	}

	public void startEcs(int port, int numberOfNodes, int cacheSize, String displacementStrategy){
		try {
			
			initService(numberOfNodes, cacheSize, displacementStrategy);
			
			ecsServer = new ServerSocket(port);
			Socket kvserver = null;

			int currentNode = 0;
			while(currentNode != numberOfNodes && (kvserver = ecsServer.accept()) != null){
				logger.info(kvserver.getInetAddress() + " " + kvserver.getPort() + " is connected");

				byte [] b = new byte[64];
				KVAdminMessage msg = new KVAdminMessage();
				kvserver.getInputStream().read(b);
				msg = msg.deserialize(new String(b, "UTF-8"));
				int receivedport = msg.getPort();

				
				for(Server server : workingservers){
					
					if( receivedport == Integer.parseInt(server.port) && 
							kvserver.getInetAddress().toString().equals("/" + server.ip)){
						hashservers.put(server.hashedkey, kvserver);						
						
						ServerConnection connection = new ServerConnection (this, kvserver.getInputStream(), kvserver.getOutputStream(),cacheSize, displacementStrategy, metadata, logger);
						hashthreads.put(server.hashedkey, connection);
						connection.start();
						currentNode++;

						break;
					}
				}
			}
//			addNode(3, "FIFO");

			while(running);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}
}
