package metadata;

import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import ecs.ConsistentHashing;
import ecs.ECS;
import ecs.Server;

public class Metadata implements Serializable{
	private List<Server> servers = new LinkedList<Server>();
	public void add(Server server){
		servers.add(server);
	}

	public int size(){
		return servers.size();
	}
	public Server putServer(Server server){
		if(servers.size() == 1){
			server.from = servers.get(0).hashedkey;
			server.to = server.hashedkey;

			servers.get(0).from = server.hashedkey;
			servers.get(0).to = servers.get(0).hashedkey;

			if(server.hashedkey.compareTo(servers.get(0).hashedkey) < 0){
				servers.add(0, server);
				return servers.get(1);

			}else{
				servers.add(1, server);
				return servers.get(0);
			}
		}
		if(server.hashedkey.compareTo(servers.get(0).from) > 0 || server.hashedkey.compareTo(servers.get(0).to) < 0){
			server.from = servers.get(0).from;
			server.to = server.hashedkey;

			servers.add(0, server);
			server.from = servers.get(1).from;
			server.to = server.hashedkey;
			servers.get(1).from = server.hashedkey;
			
			return servers.get(1);			
		}
		
		for(int i = 1; i < servers.size(); i++){
			if(servers.get(i).from.compareTo(server.hashedkey) < 0 && servers.get(i).to.compareTo(server.hashedkey) > 0){
				server.from = servers.get(i).from;
				server.to = server.hashedkey;

				servers.add(i, server);
				server.from = servers.get(i+1).from;
				server.to = server.hashedkey;

				servers.get(i+1).from = server.hashedkey;
				return servers.get(i+1);
			}
		}
		return null;
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
			return new String [] {servers.get(0).ip, servers.get(0).port};
		ConsistentHashing conHashing;
		try {
			conHashing = new ConsistentHashing();
			String hashedkey = conHashing.getHashedKey(key);

			for(int i = 0; i < servers.size(); i++)
			{
				Server server = servers.get(i);
				
				if(i==0){
					if(hashedkey.compareTo(server.from) > 0 || hashedkey.compareTo(server.to) < 0)
						return new String [] {server.ip, server.port};
				}
				
				if(hashedkey.compareTo(server.from) >= 0 && hashedkey.compareTo(server.to) <= 0){
					return new String[]{server.ip, server.port};
				}
			}

		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
		
	public Server remove(Server toRemove){
		
		if(servers.size() == 1){
			servers.remove(toRemove);
			return null;
		}

		if(servers.size() == 2){
			servers.remove(toRemove);
			servers.get(0).from = ECS.start;
			servers.get(0).to = ECS.end;

			return servers.get(0);
		}
		
		for(int i = 0; i < servers.size(); i++){
			if(servers.get(i) == toRemove){
				if(i == servers.size() - 1){
					servers.get(0).from = toRemove.from;
					servers.remove(toRemove);
					return servers.get(0);
				}
				else{
					servers.get(i+1).from = toRemove.from;
					servers.remove(toRemove);
					return servers.get(i+1);
				}
			}
		}
		return null;
	}
	
	@Override
	public String toString(){
		String result = "";
		for(Server server : servers){
			result += "{"+server.ip + " " + server.port + " " + server.from + " " + server.to+"}";
			result += "<";
		}
		return result;
	}
}
