package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

class Server{
	public String ip;
	public int port;
}

class Range{
	public String from;
	public String to;
}

public class ECS {
	
	Logger logger = Logger.getRootLogger();
	
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
}
