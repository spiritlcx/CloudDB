package strategy;

public class LFUStrategy extends Strategy {

	private final int size;
	private String[] keys;
	private int[] ratings;
	
	public LFUStrategy(int size){
		this.size = size;
		keys = new String[size];
		ratings = new int[size];
		
		for(int i = 0; i < size; i++)
		{
			ratings[i] = -1;
		}
	}
	
	@Override
	public String get() {
		int lowestRating = 0;
		int lowestRatingIndex = -1;
		
		for(int i = 0; i < size; i++)
		{
			if(ratings[i] > -1 && ratings[i] < lowestRating){
				lowestRating = ratings[i];
				lowestRatingIndex = i;
			}
		}
		
		if(lowestRatingIndex > -1)
		{
			String ret = keys[lowestRatingIndex];
			remove(ret);
			return ret;
		}
		return null;
	}

	@Override
	public void add(String key) {
		
		int containedIndex = contains(key);
		
		if(containedIndex >= 0){
			ratings[containedIndex]++;
		}
		
		else{
			for(int i = 1; i < size - 1; i++)
			{
				if(keys[i] == null && ratings[i] == -1)
				{
					keys[i] = key;
					ratings[i] = 1;
				}
			}
		}
	}
	
	private int contains(String key)
	{
		int doesContain = -1;
		
		for(int i = 0; i < size; i++)
		{
			if(keys[i] == key){
				doesContain = i;
			}
		}
		
		return doesContain;
	}
	
	private void remove(String key)
	{
		boolean copyFromHere = false;
		
		for(int i = 0; i < size; i++)
		{
			if(keys[i] == key){
				copyFromHere = true;
			}
			if(copyFromHere && (i < size - 1))
			{
				keys[i] = keys[i+1];
				ratings[i] = ratings[i+1];
			}
		}
		keys[size - 1] = null;
		ratings[size - 1] = -1;
	}

}
