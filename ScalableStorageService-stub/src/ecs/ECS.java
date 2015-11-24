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
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.StatusType;
import logger.LogSetup;
import metadata.Metadata;

class ServerHash implements Comparator<ServerHash>{
	String ip;
	String port;
	String hashedkey;
	@Override
	public int compare(ServerHash o1, ServerHash o2) {
		// TODO Auto-generated method stub
		return o1.hashedkey.compareTo(o2.hashedkey);
	}	
}

public class ECS {
	
	private Logger logger = Logger.getRootLogger();
	private Metadata metadata = new Metadata();
	private static String start = "00000000000000000000000000000000";
	private static String end = "ffffffffffffffffffffffffffffffff";

	private ServerSocket ecsServer;

	private TreeMap<String, Socket> hashservers = new TreeMap<String, Socket>();
	private ArrayList<ServerHash> servers;
	
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
				ConsistentHashing conHashing = new ConsistentHashing();
				int currentnum = 0;
				while(currentnum < numberOfNodes && (line = reader.readLine()) != null){
					String [] node = line.split(" ");
					if(node != null && node.length == 3){
						ServerHash serverHash = new ServerHash();
						serverHash.ip = node[1];
						serverHash.port = node[2];
						conHashing.add(serverHash);
						currentnum++;
					}
				}
				reader.close();

				if(currentnum == 0)
					return;
				
				servers = conHashing.distribute();

				//After receiving servers with hashed keys, it will know how to map keys to
				//each server with the range (from, to) in each server, and store the information
				//in metadata which will be used by both client and server
				
				for(int i = 0; i < servers.size(); i++){
					if(servers.size() == 1){
						metadata.add(servers.get(i).ip, servers.get(i).port, start, end);
						break;
					}
					if(i == 0)
						metadata.add(servers.get(i).ip, servers.get(i).port,start, servers.get(i).hashedkey);
					else if(i == servers.size() - 1)
						metadata.add(servers.get(i).ip, servers.get(i).port, servers.get(i).hashedkey, end);
					else
						metadata.add(servers.get(i).ip, servers.get(i).port, servers.get(i).hashedkey, servers.get(i+1).hashedkey);
				}
				
				System.out.println(metadata);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.getMessage());
		}
	}
	
	
	/* Starts the storage service by calling start() on all KVServer 
		instances that participate in the service. 
	 */
	public void start(){
		
	}
	
	/* Stops the service; all participating KVServers are stopped for 
		processing client requests but the processes remain running. 
	 */
	
	public void stop(){
		
	}
	
	
	/* Stops all server instances and exits the remote processes. 
	 */
	public void shutDown(){
		
	}

	/*Create a new KVServer with the specified cache size and 
	 * displacement strategy and add it to the storage service at an 
	 * arbitrary position. 
	 */
	public void addNode(int cacheSize, String displacementStrategy){
		
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
				ecs.startEcs(40000, 3, 3, "FIFO");
			}
		}.start();
	}

	public void startEcs(int port, int numberOfNodes, int cacheSize, String displacementStrategy){
		try {
			
			initService(numberOfNodes, cacheSize, displacementStrategy);
			
			ecsServer = new ServerSocket(port);
			Socket kvserver = null;
			KVAdminMessage initMessage = new KVAdminMessage();
			initMessage.setCacheSize(cacheSize);
			initMessage.setDisplacementStrategy(displacementStrategy);
			initMessage.setMetadata(metadata);
			initMessage.setStatusType(StatusType.INIT);
			while( (kvserver = ecsServer.accept()) != null){
				logger.info(kvserver.getInetAddress() + " " + kvserver.getPort() + " is connected");

				byte [] b = new byte[64];
				KVAdminMessage msg = new KVAdminMessage();
				kvserver.getInputStream().read(b);
				msg = msg.deserialize(new String(b, "UTF-8"));
				int receivedport = msg.getPort();

				
				for(ServerHash server : servers){
					
					if( receivedport == Integer.parseInt(server.port) && 
							kvserver.getInetAddress().toString().equals("/" + server.ip)){
						hashservers.put(server.hashedkey, kvserver);

						kvserver.getOutputStream().write((initMessage.serialize()).getBytes());
						kvserver.getOutputStream().flush();
						
						KVAdminMessage m = new KVAdminMessage();
						m.setStatusType(StatusType.START);
						kvserver.getOutputStream().write(m.serialize().getBytes());
						kvserver.getOutputStream().flush();						
						
						break;
					}
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
	}
}
