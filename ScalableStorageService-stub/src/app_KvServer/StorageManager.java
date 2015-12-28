package app_KvServer;
import java.util.HashMap;

import org.apache.log4j.Logger;

import metadata.Metadata;
import strategy.Strategy;
import common.messages.KVMessage.StatusType;
import ecs.Server;

public class StorageManager {
	
	private Metadata metadata;
    private HashMap<String, String> keyvalue;
	private Strategy strategy;
	private int cacheSize;
	private Persistance persistance;
	private Logger logger;

	private static StorageManager storageManager = null;
	
	public static StorageManager getInstance(HashMap<String, String> keyvalue, Metadata metadata, Strategy strategy, int cacheSize, Persistance persistance, Logger logger){
		if(storageManager == null){
			synchronized(StorageManager.class){
				if(storageManager == null){
					storageManager = new StorageManager(keyvalue, metadata, strategy, cacheSize, persistance, logger);
				}
			}
		}
		return storageManager;
	}
	
	private StorageManager(HashMap<String, String> keyvalue, Metadata metadata, Strategy strategy, int cacheSize, Persistance persistance, Logger logger){
		this.keyvalue = keyvalue;
		this.metadata = metadata;
		this.strategy = strategy;
		this.persistance = persistance;
		this.cacheSize = cacheSize;
		this.logger = logger;
	}
	
	public StatusType put(String key, String value){
		
		String[] server = metadata.getServer(key);
					
		if(value.equals("null")){
			if(keyvalue.get(key) != null){

				synchronized(keyvalue){
					keyvalue.remove(key);
				}
				synchronized(strategy){
					strategy.remove(key);
				}
				String keyToLoad = null;
				if((keyToLoad = persistance.read()) != null){
					String valueToLoad = persistance.lookup(keyToLoad);
					synchronized(keyvalue){
						keyvalue.put(keyToLoad, valueToLoad);
					}
					persistance.remove(keyToLoad);
					synchronized(strategy){
						strategy.add(keyToLoad);
					}
				}
			
				return StatusType.DELETE_SUCCESS;
			} 
			else if(persistance.lookup(key) != null){
				String removeMessage = persistance.remove(key);
				logger.info(removeMessage);
				if(removeMessage.contains("succesfully")){
					return StatusType.DELETE_SUCCESS;
				}
				else{
					return StatusType.DELETE_ERROR;
				}
			}
			else{
				return StatusType.DELETE_ERROR;
			}
		}
		else{
		
			if(keyvalue.get(key) != null){
				synchronized(keyvalue){
					keyvalue.put(key, value);
				}
				synchronized(strategy){
					strategy.update(key);
				}
				return StatusType.PUT_UPDATE;
			}else{
				if(persistance.lookup(key) != null){
					String removeMessage = persistance.remove(key);
					logger.info(removeMessage);
				
					//remove a key in keyvalue and put the new key						
					if(removeMessage.contains("succesfully")){
						String keytoremove = strategy.get();
						if(keytoremove != null){
							String valuetoremove = keyvalue.get(keytoremove);
							persistance.store(keytoremove, valuetoremove);
							synchronized(strategy){
								strategy.remove(keytoremove);
							}
							keyvalue.remove(keytoremove);
						}

						keyvalue.put(key, value);
						return StatusType.PUT_UPDATE;
					}
					else{
						return StatusType.PUT_ERROR;
					}
				}else{
					if(keyvalue.size() >= cacheSize){
						String keytoremove = strategy.get();
						String valuetoremove = keyvalue.get(keytoremove);
						persistance.store(keytoremove, valuetoremove);
						synchronized(keyvalue){
							keyvalue.remove(keytoremove);
							keyvalue.put(key, value);
						}
						synchronized(strategy){
							strategy.remove(keytoremove);
							strategy.add(key);
						}
					}else{
						synchronized(keyvalue){
							keyvalue.put(key, value);
						}
						synchronized(strategy){
							strategy.add(key);
						}

					}
					return StatusType.PUT_SUCCESS;
				}
			}
		}
	}

	
	public boolean isCoordinator(String ip, String port, String key){
		String [] server = metadata.getServer(key);
		return server != null && (server[0]).equals(ip) && server[1].equals(port);
	}
	
	public boolean isReplica(String ip, String port, String key){
		String [] server = metadata.getServer(key);
		Server successor = metadata.getSuccessor(server[2]);
		Server sesuccessor = metadata.getSuccessor(successor.hashedkey);
		return server != null && ((sesuccessor.ip.equals(ip) && sesuccessor.port.equals(port)) || (successor.ip.equals(ip) && successor.port.equals(port)));
	}
}
