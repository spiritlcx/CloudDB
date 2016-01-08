package store;
import java.util.ArrayList;
import common.messages.KVMessage.StatusType;
import ecs.ConsistentHashing;

public class StorageManager {	
    private Cache cache;
	private Persistance persistance;

	private static StorageManager storageManager = null;
	
	public static StorageManager getInstance(){
		return storageManager;
	}
	
	public static StorageManager getInstance(String name, String strategyName, int cacheSize){
		if(storageManager == null){
			synchronized(StorageManager.class){
				if(storageManager == null){
					storageManager = new StorageManager(name, strategyName, cacheSize);
				}
			}
		}
		return storageManager;
	}
	
	private StorageManager(String name, String strategyName, int cacheSize){
		cache = new Cache(cacheSize, strategyName);
		persistance = new Persistance(name);
	}
	
	public ArrayList<KeyValue> get(String from, String to){
		ArrayList<KeyValue> result = new ArrayList<KeyValue>();

		cache.get(from, to);
		persistance.get(from, to);
		
		return result;
	}
	
	public String get(String key){
		String value = cache.get(key);
		if(value != null)
			return value;
		return persistance.get(key);
	}
	
	public void removeData(String from, String to){
		cache.remove(from, to);
		persistance.remove(from, to);
	}
	
	public StatusType put(String key, String value){
		StatusType type = StatusType.PUT_ERROR;

		if(value.equals("null")){
			String toremove = cache.remove(key);
			if(toremove == null){
				toremove = persistance.remove(key);
			}
			if(toremove != null){
				type = StatusType.DELETE_SUCCESS;
			}else{
				type = StatusType.DELETE_ERROR;				
			}
		}else{
			if(get(key) != null){
				type = StatusType.PUT_UPDATE;
			}else{
				type = StatusType.PUT_SUCCESS;				
			}
			try{
				if(cache.isFull()){
					KeyValue toremove = cache.remove();
					if(toremove != null){
						cache.put(key, value);
						persistance.put(toremove.getKey(), toremove.getValue());
					}else{
						persistance.put(key, value);
					}
				}else{
					cache.put(key, value);
				}
			}catch(Exception e){
				type = StatusType.PUT_ERROR;
			}
		}
		return type;
	}	
}
