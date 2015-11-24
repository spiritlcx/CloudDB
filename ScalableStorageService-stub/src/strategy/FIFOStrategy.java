package strategy;

import java.util.LinkedList;
import java.util.List;

class NoKeyException extends Exception{
	public NoKeyException(){
		super("no keys in the strategy");
	}
}

public class FIFOStrategy implements Strategy {

	private List<String> lists = new LinkedList<String>();
	
	/**
	 * Gets the first element of the list and removes it.
	 * @throws NoKeyException 
	 */
	@Override
	public String get() {
		return lists.get(0);
	}

	/**
	 * Adds an element to the end of the list if it is not 
	 * in the list already.
	 */
	@Override
	public void add(String key) {
		lists.add(key);
	}

	@Override
	public void remove(String key) {
		// TODO Auto-generated method stub
		lists.remove(key);
	}

	@Override
	public void update(String key) {
		// TODO Auto-generated method stub
		
	}

}
