package strategy;

import java.util.LinkedList;
import java.util.List;

public class LRUStrategy extends Strategy {

	private List<String> lists = new LinkedList<String>();
	
	/**
	 * Returns the last element of the list and removes it.
	 */
	@Override
	public String get() {
		if(lists.size() != 0){
			String target = lists.get(lists.size() - 1);
			lists.remove(lists.size() - 1);
			return target;
		}
		return null;
	}

	/**
	 * Removes the key from the list (if it exists) and inserts
	 * it at the beginning of the list.
	 * Least Recently Used element will thus move to the end of the list.
	 */
	@Override
	public void add(String key) {
		if(lists.contains(key)){
			lists.remove(key);
		}
		
		lists.add(0, key);
	}
	

}
