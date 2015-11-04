package strategy;

import java.util.LinkedList;
import java.util.List;

public class LRUStrategy extends Strategy {

	private List<String> lists = new LinkedList<String>();
	
	@Override
	public String get() {
		if(lists.size() != 0){
			String target = lists.get(lists.size() - 1);
			lists.remove(lists.size() - 1);
			return target;
		}
		return null;
	}

	@Override
	public void add(String key) {
		if(lists.contains(key)){
			lists.remove(key);
		}
		
		lists.add(0, key);
	}
	

}
