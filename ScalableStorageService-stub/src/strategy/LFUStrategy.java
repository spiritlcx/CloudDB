package strategy;

import java.util.HashMap;

public class LFUStrategy implements Strategy {

	private HashMap<String, Integer> keyrating;
	
	public LFUStrategy(){
		keyrating = new HashMap<String, Integer>();
	}

	/**
	 * Either inserts the key with a rating of 1
	 * if it was not previously contained in the HashMap,
	 * or increases the rating of the key by 1.
	 */
	@Override
	public void add(String key) {
		keyrating.put(key, 1);
	}

	@Override
	public void update(String key) {
		// TODO Auto-generated method stub
		keyrating.put(key, keyrating.get(key) + 1);
	}

	@Override
	public String remove() {
		Integer lowestRating = Integer.MAX_VALUE;
		String lowestKey = null;
		
		for(String key : keyrating.keySet())
		{
			if(lowestRating > keyrating.get(key))
			{
				lowestRating = keyrating.get(key);
				lowestKey = key;
			}
		}
		keyrating.remove(lowestKey);
		return lowestKey;
	}
}
