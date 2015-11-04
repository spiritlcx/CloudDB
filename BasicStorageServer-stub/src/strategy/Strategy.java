package strategy;

/**
 * Abstract class Strategy allows for any of the 3 actual strategies to be used.
 */
public abstract class Strategy {
	abstract public String get();
	abstract public void add(String key);
}
