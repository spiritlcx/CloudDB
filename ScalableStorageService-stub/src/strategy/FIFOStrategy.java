package strategy;

import java.util.LinkedList;
import java.util.List;

public class FIFOStrategy implements Strategy {

	private List<String> lists = new LinkedList<String>();
	

	/**
	 * Adds an element to the end of the list if it is not 
	 * in the list already.
	 */
	@Override
	public void add(String key) {
		lists.add(key);
	}

	@Override
	public void update(String key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String remove(){
		if(lists.size() != 0){
			String key = lists.get(0);
			lists.remove(0);
			return key;
		}else{
			return null;
		}
	}

}
