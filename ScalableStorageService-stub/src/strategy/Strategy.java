package strategy;

class NoKeyException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoKeyException(){
		super("no keys in the strategy");
	}
}

/**
 * Abstract class Strategy allows for any of the 3 actual strategies to be used.
 */
public interface Strategy {
	public void add(String key);
	public void update(String key);
	public String remove() throws IllegalStateException;
}
