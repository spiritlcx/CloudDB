package metadata;

import java.util.TreeMap;

import ecs.ConsistentHashing;
import ecs.Server;

public class Metadata{

	private TreeMap<String, Server> servers = new TreeMap<String, Server>();
	public static String start = "00000000000000000000000000000000";
	public static String end = "ffffffffffffffffffffffffffffffff";
	
	private Broker broker;

	public void clear(){
		servers.clear();
	}
	public Server getServer(String hashedkey){
		return servers.get(hashedkey);
	}
		
	public TreeMap<String, Server> getServers(){
		return servers;
	}

	public Server getFirstServer(){
		return servers.firstEntry().getValue();
	}
	
	public Server getPredecessor(String hashedkey){
		
		if(servers.size() == 1)
			return null;
		if(hashedkey.equals(servers.firstKey())){
			return servers.lastEntry().getValue();
		}else{
			return servers.get(servers.lowerKey(hashedkey));
		}
	}
	
	public Server getSuccessor(String hashedkey){
		System.out.println(servers.size());
		if(servers.size() == 1)
			return null;
		if(hashedkey.equals(servers.lastKey())){
			return servers.firstEntry().getValue();
		}else{
			return servers.get(servers.higherKey(hashedkey));
		}
	}
	
	public int size(){
		return servers.size();
	}
	
	public void add(Server server){

		server.hashedkey = ConsistentHashing.getHashedKey(server.ip + server.port);

		if(servers.size() == 0){
			server.from = start;
			server.to = end;
			servers.put(server.hashedkey, server);
			return;
		}
		
		Server successor = null;
		
		if(servers.size() == 1){
			successor = servers.firstEntry().getValue();
			server.from = successor.hashedkey;
			
			successor.to = successor.hashedkey;
		}else{
			if(servers.higherKey(server.hashedkey) != null){
				successor = servers.get(servers.higherKey(server.hashedkey));							
			}else{
				successor = servers.get(servers.firstEntry().getKey());
			}
			server.from = successor.from;
		}
		successor.from = server.hashedkey;
		server.to = server.hashedkey;
		
		servers.put(server.hashedkey, server);

	}
	
	/**
	 * This method looks up the server responsible for the hash value 
	 * of a provided key.
	 * @param key	The key that needs to be looked up.
	 * @return		A tuple of the responsible server's address and port.
	 */
	public Server getServerForKey(String key)
	{
		if(servers.size() == 1)
			return servers.lastEntry().getValue();

		if(key.compareTo(servers.lastEntry().getValue().hashedkey) > 0)
			return servers.firstEntry().getValue();
		
		return servers.ceilingEntry(key).getValue();
	}
		
	public Server remove(Server toRemove){
		
		if(servers.size() == 1){
			servers.remove(toRemove.hashedkey);
			return null;
		}

		if(servers.size() == 2){
			servers.remove(toRemove.hashedkey);
			servers.firstEntry().getValue().from = start;
			servers.firstEntry().getValue().to = end;

			return servers.firstEntry().getValue();
		}


		if(toRemove.hashedkey.equals(servers.lastKey())){
			servers.firstEntry().getValue().from = toRemove.from;
			servers.remove(toRemove.hashedkey);
			return servers.firstEntry().getValue();
		}else{
			Server successor = servers.get(servers.higherKey(toRemove.hashedkey));
			successor.from = toRemove.from;
			servers.remove(toRemove.hashedkey);
			return successor;
		}		
	}
	
	public void setBroker(String address, int port){
		broker = new Broker(address, port);
	}
	
	public String getBrokerIP(){
		return broker.getAddress();
	}
	
	public int getBrokerPort(){
		return broker.getPort();
	}
	
	@Override
	public String toString(){
		String result = "";
		for(Server server : servers.values()){
			result += "{"+server.ip + " " + server.port + " " + server.hashedkey + " " + server.from + " " + server.to+"}";
			result += "<";
		}
		return result;
	}
}

class Broker{
	private String address;
	private int port;
	
	public Broker(String address, int port){
		this.address = address;
		this.port = port;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
