package strategy;

public class LRUStrategy extends Strategy {

	private final int size;
	private String[] keys;
	
	public LRUStrategy(int size){
		this.size = size;
		keys = new String[size];
	}
	
	@Override
	public String get() {
		for(int i = size; i > 0; i--)
		{
			if(keys[i-1] != null)
			{
				String ret = keys[i-1];
				remove(ret);
				return ret;
			}
		}
		
		return null;
	}

	@Override
	public void add(String key) {
		
		String tmp;
		int doesContain = contains(key);
		
		if(doesContain >= 0){
			remove(key);
			add(key);
		}
		
		else{
			tmp = keys[0];
			keys[0] = key;
		
			for(int i = 1; i < size - 1; i++)
			{
				keys[i] = tmp;
				tmp = keys[i + 1];
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
			}
		}
		keys[size - 1] = null;
	}

}
