package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

				if(currentnum == 0)
					return;
				
				ArrayList<ServerHash> servers = conHashing.distribute();

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

		ECS ecs = new ECS();
		ecs.initService(3, 3, "FIFO");
		
	}
}
