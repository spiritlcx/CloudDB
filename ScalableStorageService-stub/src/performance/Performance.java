package performance;

import client.KVStore;
import ecs.ECS;

public class Performance {

	KVStore[] clients;
	ECS ecs;
	int cacheSize;
	String strategy;
	
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
	public Performance(int servers, int clients, int cacheSize, String strategy){
		ecs = new ECS();
		ecs.startEcs(40000, servers, cacheSize, strategy);
		ecs.start();
		
		this.clients = new KVStore[clients];
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		
		for(int i = 0; i < clients; i++)
		{
			this.clients[i] = new KVStore("127.0.0.1", 50000);
		}
	
	}
	
	public void meassurePut()
	{
		long startTime = System.currentTimeMillis();

		try{
	      clients[1].put("1", "Some message");
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
	      clients[1].get("1");
		} 
		catch(Exception e){
		}

	      long stopTime = System.currentTimeMillis();
	      long elapsedTime = stopTime - startTime;
	      System.out.println(elapsedTime);
	}
	
	public void meassureAdd(){
		long startTime = System.currentTimeMillis();

		try{
	      ecs.addNode(cacheSize, strategy);
		} 
		catch(Exception e){
		}

	      long stopTime = System.currentTimeMillis();
	      long elapsedTime = stopTime - startTime;
	      System.out.println(elapsedTime);
	}
	
	public void meassureRemove(){
		long startTime = System.currentTimeMillis();

		try{
	      ecs.removeNode();
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
		if(args.length == 4)
		{
			Performance performance = new Performance(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
			performance.meassurePut();
			performance.meassureGet();
			performance.meassureAdd();
			performance.meassureRemove();
		}

	}

}
