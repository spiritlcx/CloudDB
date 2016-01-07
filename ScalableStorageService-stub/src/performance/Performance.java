package performance;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import app_kvServer.KVServer;
import client.KVStore;

public class Performance {

	KVStore[] clients;
	
	/**
	 * Class that can be used for scaled performance measurements.
	 * It requires an existing configuration file for the ECS with
	 * 10 server names.
	 * The constructor of this class sets up the performance testing
	 * framework.
	 * @param servers	Number of servers
	 * @param clients	Number of clients
	 * @param cacheSize	Desired cacheSize on each server
	 * @param strategy	Storage strategy
	 */
	public Performance(int clients){
		this.clients = new KVStore[clients];
		
		for(int i = 0; i < clients; i++)
		{
			this.clients[i] = new KVStore("127.0.0.1", 50004);
			try {
				this.clients[i].connect();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
	}
	
	public void finish(){
		for(int i = 0; i < clients.length; i++)
		{
			this.clients[i].disconnect();
		}

	}
	
	public void meassurePut()
	{
		long startTime = System.currentTimeMillis();

		try{
			for(int i = 0; i < clients.length; i++){
				for(int j = 0; j < 20000; j++){
					clients[i].put(""+i+j, "Some message");
				}
			}
		} 
		catch(Exception e){
		}

	      long stopTime = System.currentTimeMillis();
	      long elapsedTime = stopTime - startTime;
	      System.out.println(elapsedTime);
	}
	
	public void meassureGet()
	{
		long startTime = System.currentTimeMillis();

		try{
			for(int i = 0; i < clients.length; i++){
				for(int j = 0; j < 20000; j++){
					clients[i].get(""+i+j);
				}
			}
		} 
		catch(Exception e){
		}

	      long stopTime = System.currentTimeMillis();
	      long elapsedTime = stopTime - startTime;
	      System.out.println(elapsedTime);
	}
		
	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		if(args.length == 1)
//		{
			Performance performance = new Performance(1);
//			performance.meassurePut();
			performance.meassureGet();
//			performance.meassureAdd();
//			performance.meassureRemove();
			
			performance.finish();
			
			
//		}

	}

}
