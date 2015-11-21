package strategy;

public class StrategyFactory {
	/**
	 * Returns a strategy according to the provided String input.
	 * @param strategy Strategy name: FIFO | LFU | LRU
	 * @return Strategy suiting the input String or null.
	 */
	public static Strategy getStrategy(String strategy){
		if(strategy.equals("FIFO")){
			return new FIFOStrategy();
		}
		else if(strategy.equals("LRU")){
			return new LRUStrategy();
		}
		else if(strategy.equals("LFU")){
			return new LFUStrategy();
		}

		return null;
	}
}
