package strategy;

/**
 * Abstract class Strategy allows for any of the 3 actual strategies to be used.
 */
public interface Strategy {
	public String get();
	public void add(String key);
	public void remove(String key);
	public void update(String key);
}
