package store;

import ecs.ConsistentHashing;

public class Storage {
	protected boolean inRange(String key, String from, String to){
		if(from.compareTo(to) > 0){
			if(ConsistentHashing.getHashedKey(key).compareTo(from) > 0 || 
					ConsistentHashing.getHashedKey(key).compareTo(to) < 0){
				return true;
			}
		}else{
			if(ConsistentHashing.getHashedKey(key).compareTo(from) > 0 && 
					ConsistentHashing.getHashedKey(key).compareTo(to) < 0){
				return true;
			}
		}
		return false;
	}

}
