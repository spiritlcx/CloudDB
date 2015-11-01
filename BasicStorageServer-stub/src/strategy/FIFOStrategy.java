package strategy;

import java.util.LinkedList;
import java.util.List;

public class FIFOStrategy extends Strategy {

	private List<String> lists = new LinkedList<String>();
	
	@Override
	public String get() {
		// TODO Auto-generated method stub
		if(lists.size() != 0){
			String target = lists.get(0);
			lists.remove(0);
			return target;
		}
		return null;
	}

	@Override
	public void add(String key) {
		// TODO Auto-generated method stub
		lists.add(key);
	}

}
