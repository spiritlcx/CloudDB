package strategy;

public class StrategyFactory {
	public static Strategy getStrategy(String strategy){
		if(strategy.equals("FIFO")){
			return new FIFOStrategy();
		}else if(strategy.equals("LRU")){
			return new LRUStrategy();
		}else if(strategy.equals("LFU")){
			return new LFUStrategy();
		}

		return null;
	}
}
