package strategy;

public class StrategyFactory {
	public static Strategy getStrategy(String strategy, int size){
		if(strategy.equals("FIFO")){
			return new FIFOStrategy();
		}
		else if(strategy.equals("LRU")){
			return new LRUStrategy(size);
		}
		else if(strategy.equals("LFU")){
			return new LFUStrategy(size);
		}

		return null;
	}
}
