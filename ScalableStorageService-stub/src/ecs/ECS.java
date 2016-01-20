package ecs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import metadata.Metadata;

public class ECS {

	final private Logger logger = Logger.getRootLogger();
	private Metadata metadata = new Metadata();
	private ServerSocket ecsServer;
	private DatagramSocket detectorServer;
	private volatile boolean running;
	
	private HashMap<String, ServerConnection> hashthreads = new HashMap<String, ServerConnection>();

	private ArrayList<Server> idleservers = new ArrayList<Server>();

	ArrayList<Process> procs = new ArrayList<Process>();
	
	

	
	public ECS(){
		running = true;
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

								String hashedkey = ConsistentHashing.getHashedKey(ipport[0] + ipport[1]);

								if(hashthreads.remove(hashedkey) != null){
									final Server toremove = metadata.getServer(hashedkey);

									logger.info("server with ip:" + ipport[0] + " and port:" + ipport[1] + " has failed");

									Thread t = new Thread(){
										public void run(){
											handleFailure(toremove);
										}
									};
									t.start();
									t.join();
									
									addNode(10, "FIFO");
								}
							}
							
						} catch (IOException | InterruptedException e) {
							// TODO Auto-generated catch block
							logger.info("failuredetector is closed");
							return;
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
					if(node.length == 3){
						idleservers.add(new Server(node[1], node[2]));
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
					int index = random.nextInt(idleservers.size());
					metadata.add(idleservers.get(index));
					idleservers.remove(index);
				}
								
				for(Server server : metadata.getServers().values()){
					logger.info(server);
				}
				
				metadata.setBroker("127.0.0.1", 49999);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}

	}
	
	public TreeMap<String, Server> getServers(){
		return metadata.getServers();
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
				
				hashthreads.clear();
				idleservers.clear();
				metadata.clear();
				detectorServer.close();
				try {
					ecsServer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();

		stopServers();

	}

	/*
	 * stop the ECS itself
	 */
	public void stopRunning(){
		running = false;

		detectorServer.close();
		try {
			ecsServer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void handleFailure(Server server){
		idleservers.add(server);

		//don't need to move data if the number of servers is less than or equal to 3
		if(metadata.size() > 3){
			Server preServer = metadata.getPredecessor(server.hashedkey);
	
			Server sepreServer = metadata.getPredecessor(preServer.hashedkey);
	
			Server suceServer = metadata.getSuccessor(server.hashedkey);
			Server sesuceServer = metadata.getSuccessor(suceServer.hashedkey);
			Server thsuceServer = metadata.getSuccessor(sesuceServer.hashedkey);
	
			hashthreads.get(suceServer.hashedkey).receiveData();
			hashthreads.get(sesuceServer.hashedkey).receiveData();
	
			hashthreads.get(preServer.hashedkey).moveData(preServer.from, preServer.to, sesuceServer.ip, Integer.parseInt(sesuceServer.port) - 20);
			hashthreads.get(sepreServer.hashedkey).moveData(sepreServer.from, sepreServer.to, suceServer.ip, Integer.parseInt(suceServer.port) - 20);
					
			// thsuceServer and suceServer is same when size is 4
			if(metadata.size() != 4){
				hashthreads.get(thsuceServer.hashedkey).receiveData();
				hashthreads.get(suceServer.hashedkey).moveData(suceServer.from, suceServer.to, thsuceServer.ip, Integer.parseInt(thsuceServer.port) - 20);
			}
		}
		
		metadata.remove(server);
			
		for(ServerConnection connection : hashthreads.values()){
			connection.update(metadata);
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
					Runtime run = Runtime.getRuntime(); 

					Process proc;
					try {
						proc = run.exec("java -jar ./ms3-server.jar " + newworkingserver.port);
						procs.add(proc);

					} catch (IOException e1) {
						// TODO Auto-generated catch block
						logger.error(e1.getMessage());
					}

					Server successor = metadata.getSuccessor(newworkingserver.hashedkey);
		
					try {
						Socket kvserver = null;
						while((kvserver = ecsServer.accept()) != null){					
//							if(kvserver.getInetAddress().toString().equals("/" + newworkingserver.ip) && kvserver.getPort() == Integer.parseInt(newworkingserver.port)){
							ServerConnection connection = new ServerConnection(kvserver, newcacheSize, displacementStrategy, metadata, logger);
							hashthreads.put(newworkingserver.hashedkey, connection);
				
							int port = connection.init();
							if(port != Integer.parseInt(newworkingserver.port))
								continue;
							connection.startServer();
							
							if(metadata.getServers().size() == 1)
								return;
							
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
					suserver.moveData(newworkingserver.from, newworkingserver.to, newworkingserver.ip, Integer.parseInt(newworkingserver.port) - 20);

					if(metadata.getServers().size() > 3){
						Server sesuserver = metadata.getSuccessor(successor.hashedkey);
						Server thsuserver = metadata.getSuccessor(sesuserver.hashedkey);
						hashthreads.get(thsuserver.hashedkey).removeData(newworkingserver.from, newworkingserver.to);
						
						Server preserver = metadata.getPredecessor(newworkingserver.hashedkey);
						Server sepreserver = metadata.getPredecessor(preserver.hashedkey);

						hashthreads.get(newworkingserver.hashedkey).receiveData();
						hashthreads.get(preserver.hashedkey).moveData(preserver.from, preserver.to, newworkingserver.ip, Integer.parseInt(newworkingserver.port) - 20);

						hashthreads.get(newworkingserver.hashedkey).receiveData();
						hashthreads.get(sepreserver.hashedkey).moveData(sepreserver.from, sepreserver.to, newworkingserver.ip, Integer.parseInt(newworkingserver.port) - 20);

					}else if(metadata.getServers().size() == 3){
						Server preserver = metadata.getPredecessor(newworkingserver.hashedkey);

						hashthreads.get(newworkingserver.hashedkey).receiveData();
						hashthreads.get(preserver.hashedkey).moveData(preserver.from, preserver.to, newworkingserver.ip, Integer.parseInt(newworkingserver.port) - 20);
					}

					for(ServerConnection connection : hashthreads.values()){
						connection.update(metadata);
					}
					
					suserver.releaseWriteLock();
				}else{
					logger.info("no server available");
				}
			}
		}.start();
	}

	/**
	 *  Remove a node from the storage service at an arbitrary position. 
	 */
	public void removeNode(){
		Thread t = new Thread(){
			public void run(){
				Server toremove = metadata.getFirstServer();
				hashthreads.get(toremove.hashedkey).shutDown();
				hashthreads.remove(toremove.hashedkey);

				logger.info(toremove.port + " is removed");
				handleFailure(toremove);

			}
		};
		t.start();
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
					
					startServers();
					
					while(currentNode != numberOfNodes && (kvserver = ecsServer.accept()) != null){
						logger.info(kvserver.getInetAddress() + " " + kvserver.getPort() + " is connected");
		
						ServerConnection connection = new ServerConnection (kvserver, cacheSize, displacementStrategy, metadata, logger);
						int receivedport = connection.init();
						
						for(Server server : metadata.getServers().values()){
							
							if( receivedport == Integer.parseInt(server.port) && 
									kvserver.getInetAddress().toString().equals("/" + server.ip)){
	
								hashthreads.put(server.hashedkey, connection);
								currentNode++;
		
								break;
							}
							else if(receivedport == metadata.getBrokerPort()){
								hashthreads.put("BrokerService", connection);
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

		metadata.add(newworkingserver);
		idleservers.remove(newworkingserver);

		return newworkingserver;
	}

	private void stopServers(){
		for(Process proc : procs)
			proc.destroy();
	}
	
	private void startServers(){
		Runtime run = Runtime.getRuntime(); 

		Process proc = null;

		for(Server server : metadata.getServers().values()){
			try {
				proc = run.exec("java -jar ./ms3-server.jar " + server.port);
				procs.add(proc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("failed");
			}
		}
		
		try {
			proc = run.exec("java -jar ./ms5-broker.jar 127.0.0.1 " + metadata.getFirstServer().port + " 49999");
			procs.add(proc);
		} catch (IOException e) {
			System.out.println("failed");
		}
	}
}
