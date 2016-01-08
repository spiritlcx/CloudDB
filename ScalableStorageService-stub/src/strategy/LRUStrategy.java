package strategy;

import java.util.LinkedList;
import java.util.List;

public class LRUStrategy implements Strategy {

	private List<String> lists = new LinkedList<String>();
	
	/**
	 * Removes the key from the list (if it exists) and inserts
	 * it at the beginning of the list.
	 * Least Recently Used element will thus move to the end of the list.
	 */
	@Override
	public void add(String key) {
		lists.add(0, key);
	}
	
	@Override
	public void update(String key) {
		// TODO Auto-generated method stub
		lists.remove(key);
		lists.add(0, key);
	}

	@Override
	public String remove() {
		// TODO Auto-generated method stub
		if(lists.size() == 0)
			return null;
		String key = lists.get(lists.size() - 1);
		lists.remove(key);
		return key;
	}
}
