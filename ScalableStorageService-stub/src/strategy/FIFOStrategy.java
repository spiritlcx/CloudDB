package strategy;

import java.util.LinkedList;
import java.util.List;

public class FIFOStrategy extends Strategy {

	private List<String> lists = new LinkedList<String>();
	
	/**
	 * Gets the first element of the list and removes it.
	 */
	@Override
	public String get() {
		if(lists.size() != 0){
			String target = lists.get(0);
			lists.remove(0);
			return target;
		}
		return null;
	}

	/**
	 * Adds an element to the end of the list if it is not 
	 * in the list already.
	 */
	@Override
	public void add(String key) {
		if(!lists.contains(key)){
			lists.add(key);
		}
	}

}
