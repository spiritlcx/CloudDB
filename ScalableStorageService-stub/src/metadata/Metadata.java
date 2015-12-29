package metadata;

import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import ecs.ConsistentHashing;
import ecs.ECS;
import ecs.Server;

public class Metadata implements Serializable{

	private TreeMap<String, Server> servers = new TreeMap<String, Server>();

	public TreeMap<String, Server> getServers(){
		return servers;
	}
	
	public void add(Server server){
		servers.put(server.hashedkey, server);
	}

	public void set(TreeMap<String, Server> servers){
		this.servers = servers;
	}
	
	public Server getSuccessor(String hashedkey){
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
	
	public Server putServer(Server server){
		
		Server successor = null;
		
		if(servers.size() == 1){
			successor = servers.firstEntry().getValue();
			server.from = successor.hashedkey;
			
			successor.to = successor.hashedkey;
		}
		
		if(servers.higherKey(server.hashedkey) !=null){
			successor = servers.get(servers.higherKey(server.hashedkey));
			server.from = successor.from;
						
		}else{
			successor = servers.get(servers.firstEntry().getKey());
			server.from = successor.from;	
		}

		successor.from = server.hashedkey;
		server.to = server.hashedkey;
		
		servers.put(server.hashedkey, server);

		return successor;
	}
	
	/**
	 * This method looks up the server responsible for the hash value 
	 * of a provided key.
	 * @param key	The key that needs to be looked up.
	 * @return		A tuple of the responsible server's address and port.
	 */
	public String[] getServer(String key)
	{
		if(servers.size() == 1)
			return new String [] {servers.lastEntry().getValue().ip, servers.lastEntry().getValue().port, servers.lastEntry().getValue().hashedkey};

		if(key.compareTo(servers.lastEntry().getValue().hashedkey) > 0)
			return new String [] {servers.firstEntry().getValue().ip, servers.firstEntry().getValue().port, servers.firstEntry().getValue().hashedkey};
		
		return new String [] {servers.ceilingEntry(key).getValue().ip, servers.ceilingEntry(key).getValue().port, servers.ceilingEntry(key).getValue().hashedkey};
	}
		
	public Server remove(Server toRemove){
		
		if(servers.size() == 1){
			servers.remove(toRemove);
			return null;
		}

		if(servers.size() == 2){
			servers.remove(toRemove);
			servers.firstEntry().getValue().from = ECS.start;
			servers.firstEntry().getValue().to = ECS.end;

			return servers.firstEntry().getValue();
		}

		
		if(toRemove.hashedkey.equals(servers.lastKey())){
			servers.firstEntry().getValue().from = toRemove.from;
			servers.remove(toRemove);
			return servers.firstEntry().getValue();
		}else{
			Server successor = servers.get(servers.higherKey(toRemove.hashedkey));
			successor.from = toRemove.from;
			servers.remove(toRemove);
			return successor;
		}		
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
