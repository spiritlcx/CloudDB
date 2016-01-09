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
		
		result.addAll(cache.get(from, to));
		result.addAll(persistance.get(from, to));
		
		return result;
	}
	
	public String get(String key){
		String value = cache.get(key);
		if(value != null)
			return value;
		value = persistance.get(key);
		if(cache.isFull()){
			KeyValue topersist = cache.remove();
			persistance.put(topersist.getKey(), topersist.getValue());
		}

		try {
			cache.put(key, value);
		} catch (CacheFullException e) {
			// TODO Auto-generated catch block
			return null;
		}
		return value;
	}
	
	public void removeData(String from, String to){
		cache.remove(from, to);
		persistance.remove(from, to);
	}
	
	public synchronized StatusType put(String key, String value){
		StatusType type = StatusType.PUT_ERROR;

		if(key == null || value == null){
			return type;
		}
		
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
				put(key, "null");
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
	public static void main(String [] args){
		StorageManager storageManager = new StorageManager("aaa", "FIFO", 100);
//		for(int i = 0; i < 200000; i++){
//			storageManager.put(i+"", i+"");
//		}
		System.out.println(ConsistentHashing.getHashedKey("aa"));
		System.out.println(ConsistentHashing.getHashedKey("bb"));

		ArrayList<KeyValue> m = storageManager.get(ConsistentHashing.getHashedKey("bb"), ConsistentHashing.getHashedKey("aa"));
		for(KeyValue ke : m){
			System.out.println(ke.getKey());
		}
		
	}
}
