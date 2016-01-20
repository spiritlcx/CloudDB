package notification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class handles the data that connects clients to their subscriptions.
 * Only online subscribers are handled, data of other subscribers is stored
 * in files.
 * 
 * In a real large scale scenario, the proposed mechanism might be very slow.
 * The subscriber data could be split by certain criteria (number of subscribers
 * to a certain key, key ranges) and stored in database tables for faster access.
 * Furthermore, since the possible number of subscriptions is the number of 
 * subscribers multiplied with the number of keys, a separate distributed subscriber
 * management system might be necessary. 
 */

public class SubscriberData {
	private HashMap<String, List<BrokerConnection>> subscriptions;
	private HashMap<BrokerConnection, List<String>> subscribers;
	
	public boolean addSubscription(String key, BrokerConnection client)
	{
		try{
			if(subscriptions.containsKey(key) == true){
				subscriptions.get(key).add(client);
			} else{
				List<BrokerConnection> tmp = new ArrayList<BrokerConnection>();
				tmp.add(client);
				subscriptions.put(key, tmp);
			}
			
			if(subscribers.containsKey(client) == true){
				subscribers.get(client).add(key);
			} else{
				List<String> tmp = new ArrayList<String>();
				tmp.add(key);
				subscribers.put(client, tmp);
			}
			
			return true;
		} catch(Exception e){
			return false;
		}		
	}
	
	public boolean removeSubscription(String key, BrokerConnection client)
	{
		if(subscriptions.containsKey(key) == true){
			List<BrokerConnection> tmp = subscriptions.get(key);
			tmp.remove(client);
			
			//If this was the last subscriber, remove entry
			if(tmp.isEmpty()){
				subscriptions.remove(key);
			}
			
			if(subscribers.containsKey(client) == true){
				List<String> tmp2 = subscribers.get(client);
				tmp2.remove(key);
				
				//If this was the last subscriber, remove entry
				if(tmp2.isEmpty()){
					subscribers.remove(client);
				}
				
				return true;
			}
			else{
				return false;
			}
		}
		else{
			return false;
		}
	}
	
	public List<BrokerConnection> getSubscribers(String key){
		if(subscriptions.containsKey(key)){
			return subscriptions.get(key);
		} else{
			return null;
		}
	}
	
	public boolean removeSubscriber(BrokerConnection subscriber)
	{
		boolean b = true;
		if(subscribers.containsKey(subscriber)){
			List<String> keys = subscribers.get(subscriber);
			
			for(String key: keys){
				if(subscriptions.containsKey(key) == true){
					List<BrokerConnection> tmp = subscriptions.get(key);
					tmp.remove(subscriber);
				} else{
					b = false;
				}
			}
			
			subscribers.remove(subscriber);
		} 
		
		return b;
	}
}
 