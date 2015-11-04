package strategy;

import java.util.HashMap;

public class LFUStrategy extends Strategy {

	private HashMap<String, Integer> keyrating;
	
	public LFUStrategy(){
		keyrating = new HashMap<String, Integer>();
	}
	
	@Override
	public String get() {
		Integer lowestRating = Integer.MAX_VALUE;
		String lowestKey = "";
		
		for(String key : keyrating.keySet())
		{
			if(lowestRating > keyrating.get(key))
			{
				lowestRating = keyrating.get(key);
				lowestKey = key;
			}
		}
		
		if(!lowestKey.equals(""))
		{
			keyrating.remove(lowestKey);
			return lowestKey;
		}
		return null;
	}

	@Override
	public void add(String key) {
		
		if(keyrating.containsKey(key)){
			Integer rating = keyrating.get(key) + 1;
			keyrating.remove(key);
			keyrating.put(key, rating);
		}
		else
		{
			keyrating.put(key, 1);
		}
	}
}
