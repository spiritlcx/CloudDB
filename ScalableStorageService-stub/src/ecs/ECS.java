package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.StatusType;
import metadata.Metadata;

public class ECS {

	final private Logger logger = Logger.getRootLogger();
	private Metadata metadata = new Metadata();
	public static String start = "00000000000000000000000000000000";
	public static String end = "ffffffffffffffffffffffffffffffff";
	private ServerSocket ecsServer;
	DatagramSocket detectorServer;
	boolean running;
	
	private HashMap<String, ServerConnection> hashthreads = new HashMap<String, ServerConnection>();
	private ConsistentHashing conHashing;

	private TreeMap<String, Server> workingservers = new TreeMap<String, Server>();
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
	
	/**
	 * Randomly choose <numberOfNodes> servers from the available 
	 * machines and start the KVServer by issuing a SSH call to the 
	 * respective machine. This call launches the server with the 
	 * specified cache size and displacement strategy. You can 
	 * assume that the KVServer.jar is located in the same directory as 
	 * the ECS. All servers are initialized with the meta­data and 
	 * remain in state stopped.
	 */
	public void initService (int numberOfNodes, int cacheSize, String displacementStrategy){		
		initServers(numberOfNodes);
		initFailureDetector();
	}
	
	private void initFailureDetector(){
		try {
			detectorServer = new DatagramSocket(20000);
			new Thread(){
				public void run(){
					while(running){
						byte[] receiveData = new byte[128];
						DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
						try {
							detectorServer.receive(receivePacket);
							String failure = new String(receiveData);
							failure = failure.trim();
							String [] ipport = failure.split(" ");
							
							if(ipport.length == 2){

								String hashedkey = conHashing.getHashedKey(ipport[0] + ipport[1]);
								
								Server toremove = workingservers.get(hashedkey);
								if(toremove != null){
									hashthreads.remove(hashedkey);
									idleservers.add(toremove);
	
									logger.info("server with ip:" + ipport[0] + " and port:" + ipport[1] + " has failed");
	
									handleFailure(toremove);
									
									workingservers.remove(hashedkey);
									
									for(ServerConnection connection : hashthreads.values()){
										connection.update(metadata);
									}
								}
							}
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}					
				}
			}.start();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			logger.error("failure detector cannot be initialized");
		}
	}
	
	private void initServers(int numberOfNodes){
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
				
				workingservers = conHashing.getServers();

				//After receiving servers with hashed keys, it will know how to map keys to
				//each server with the range (from, to) in each server, and store the information
				//in metadata which will be used by both client and server


				for(String key : workingservers.keySet()){
					if(workingservers.size() == 1){
						workingservers.get(key).from = start;
						workingservers.get(key).to = end;
						break;
					}
					
					
					workingservers.get(key).to = key;

					String formerkey = workingservers.lowerKey(key);

					if(formerkey == null){
						formerkey = workingservers.lastKey();
					}
					workingservers.get(key).from = workingservers.get(formerkey).hashedkey;					
				}

				metadata.set(workingservers);
				
				for(Server server : workingservers.values()){
					logger.info(server);
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
	
	/**
	 *  Starts the storage service by calling start() on all KVServer 
	 *  instances that participate in the service. 
	 */
	public void start(){
		new Thread(){
			public void run(){
				for(String key : hashthreads.keySet()){
					ServerConnection connection = hashthreads.get(key);
					connection.startServer();
				}
			}
		}.start();
	}
	
	/**
	 *  Stops the service; all participating KVServers are stopped for 
	 *  processing client requests but the processes remain running. 
	 */
	
	public void stop(){
		new Thread(){
			public void run(){
				for(String key : hashthreads.keySet()){
					ServerConnection connection = hashthreads.get(key);
					connection.stopServer();
				}
			}
		}.start();
	}
	
	
	/**
	 *  Stops all server instances and exits the remote processes. 
	 */
	public void shutDown(){
		new Thread(){
			public void run(){
				for(String key : hashthreads.keySet()){
					ServerConnection connection = hashthreads.get(key);
					connection.shutDown();
				}
			}
		}.start();
	}

	/*
	 * stop the ECS itself
	 */
	public void stopRunning(){
		running = false;
	}
	
	public void handleFailure(Server server){
		
		if(workingservers.size() < 3)
			return;
		
		Server preServer = metadata.getPredecessor(server.hashedkey);

		Server sepreServer = metadata.getPredecessor(preServer.hashedkey);

		Server suceServer = metadata.getSuccessor(server.hashedkey);
		Server sesuceServer = metadata.getSuccessor(suceServer.hashedkey);
		Server thsuceServer = metadata.getSuccessor(sesuceServer.hashedkey);

		hashthreads.get(suceServer.hashedkey).receiveData();
		hashthreads.get(sesuceServer.hashedkey).receiveData();

		hashthreads.get(preServer.hashedkey).moveData(preServer.from, preServer.to, suceServer.ip, Integer.parseInt(suceServer.port) - 20);
		hashthreads.get(sepreServer.hashedkey).moveData(sepreServer.from, sepreServer.to, sesuceServer.ip, Integer.parseInt(sesuceServer.port) - 20);
		
		
		// thsuceServer and suceServer is same when size is 4
		if(workingservers.size() != 4){
			hashthreads.get(thsuceServer.hashedkey).receiveData();
			hashthreads.get(suceServer.hashedkey).moveData(suceServer.from, suceServer.to, thsuceServer.ip, Integer.parseInt(thsuceServer.port) - 20);
		}
			
	}
	
	 /* Create a new KVServer with the specified cache size and 
	 * displacement strategy and add it to the storage service at an 
	 * arbitrary position. 
	 */
	public void addNode(final int newcacheSize, final String displacementStrategy){
		//If there are idle servers in the repository, randomly pick one of 
		//them and send an SSH call to invoke the KVServer process.

		new Thread(){
			public void run(){
		
				if(idleservers.size() != 0){
					Server newworkingserver = getRandomNode();
					
					Server successor = metadata.putServer(newworkingserver);
		
					try {
						Socket kvserver = null;
						while((kvserver = ecsServer.accept()) != null){					
//							if(kvserver.getInetAddress().toString().equals("/" + newworkingserver.ip) && kvserver.getPort() == Integer.parseInt(newworkingserver.port)){
							ServerConnection connection = new ServerConnection(kvserver.getInputStream(), kvserver.getOutputStream(),newcacheSize, displacementStrategy, metadata, logger);
							hashthreads.put(newworkingserver.hashedkey, connection);
				
							int port = connection.init();
							if(port != Integer.parseInt(newworkingserver.port))
								continue;
							connection.startServer();
							connection.receiveData();

							break;
							
//							}
						}
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						logger.error(e.getMessage());
					}
		
					ServerConnection suserver = hashthreads.get(successor.hashedkey);
					
					suserver.setWriteLock();
					suserver.moveData(newworkingserver.from, newworkingserver.to, newworkingserver.ip, Integer.parseInt(newworkingserver.to) - 20);
											
					for(ServerConnection connection : hashthreads.values()){
						connection.update(metadata);
					}
					
					suserver.releaseWriteLock();
				}
			}
		}.start();
	}

	/**
	 *  Remove a node from the storage service at an arbitrary position. 
	 */
	public void removeNode(){
		
		new Thread(){
			public void run(){
				Random random = new Random();
				if(workingservers.size() == 1){
					shutDown();
				}
				int removeInt = random.nextInt(workingservers.size());
				Server toRemove = workingservers.get(removeInt);
				Server successor = metadata.remove(toRemove);

				hashthreads.get(toRemove.hashedkey).setWriteLock();
				hashthreads.get(successor.hashedkey).update(metadata);

				hashthreads.get(successor.hashedkey).receiveData();
				hashthreads.get(toRemove.hashedkey).moveData(toRemove.from, toRemove.to, successor.ip, Integer.parseInt(successor.to)-20);
				
				KVAdminMessage moveFinished = new KVAdminMessage(hashthreads.get(toRemove.hashedkey).getInput());
				moveFinished = moveFinished.deserialize(moveFinished.getMsg());
				if(moveFinished.getStatusType() == StatusType.MOVEFINISH){
					for(ServerConnection connection : hashthreads.values()){
						connection.update(metadata);
					}
				}
	
				hashthreads.get(toRemove.hashedkey).shutDown();
				
			}
		}.start();
		
	}
	
//	public static void main(String [] args){
//		try {
//			
//			new LogSetup("logs/ecs.log", Level.ALL);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		final ECS ecs = new ECS();
//		new Thread(){
//			public void run(){
//				ecs.startEcs(40000, 2, 10, "FIFO");
//			}
//		}.start();
//	}

	public TreeMap<String, Server> getServers(){
		return workingservers;
	}
	
	public Metadata getMetaData(){
		return metadata;
	}
	
	public void startEcs(final int port, final int numberOfNodes, final int cacheSize, final String displacementStrategy){
		
		new Thread(){
			public void run(){
	
				try {
					
					initService(numberOfNodes, cacheSize, displacementStrategy);
					
					ecsServer = new ServerSocket(port);
					Socket kvserver = null;
		
					int currentNode = 0;
					while(currentNode != numberOfNodes && (kvserver = ecsServer.accept()) != null){
						logger.info(kvserver.getInetAddress() + " " + kvserver.getPort() + " is connected");
		
						ServerConnection connection = new ServerConnection (kvserver.getInputStream(), kvserver.getOutputStream(),cacheSize, displacementStrategy, metadata, logger);
						int receivedport = connection.init();
						
						for(Server server : workingservers.values()){
							
							if( receivedport == Integer.parseInt(server.port) && 
									kvserver.getInetAddress().toString().equals("/" + server.ip)){
	
								hashthreads.put(server.hashedkey, connection);
								currentNode++;
		
								break;
							}
						}
					}
		
					while(running);
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage());
				}
			}
		}.start();
	}
	
	private Server getRandomNode(){
		Random random = new Random();
		Server newworkingserver = idleservers.get(random.nextInt(idleservers.size()));

		logger.info("The server with ip:"+newworkingserver.ip + " and port:"+newworkingserver.port + " is picked as a new node");

		//Recalculate and update the meta­data of the storage service
		newworkingserver.hashedkey = conHashing.getHashedKey(newworkingserver.ip + newworkingserver.port);
		workingservers.put(newworkingserver.hashedkey, newworkingserver);
		idleservers.remove(newworkingserver);

		return newworkingserver;
	}
}
