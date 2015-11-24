package metadata;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;

class Server{
	String ip;
	String port;
	String from;
	String to;
	
	public Server(String ip, String port, String from, String to){
		this.ip = ip;
		this.port = port;
		this.from = from;
		this.to = to;
	}
	
}

public class Metadata {
	private ArrayList<Server> servers = new ArrayList<Server>();
	public void add(String ip, String port, String from, String to){
		servers.add(new Server(ip, port, from, to));
	}

	/**
	 * This method looks up the server responsible for the hash value 
	 * of a provided key.
	 * @param key	The key that needs to be looked up.
	 * @return		A tuple of the responsible server's address and port.
	 */
	public String[] getServer(String key)
	{
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte[] result = md.digest();
			String hashedkey = "";
			for(int i = 0; i < 16; i++){
				int n = result[i] >> 4 & 0x0F;
		 		hashedkey += String.format("%1s", Integer.toHexString(n));
		 		n = result[i] & 0xF;
		 		hashedkey += String.format("%1s", Integer.toHexString(n));
			}
		
			for(Server server: servers)
			{
				if(hashedkey.compareTo(server.from) >= 0 && hashedkey.compareTo(server.to) <= 0){
					return new String[]{server.ip, server.port};
				}
			}
		} 
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	// randomly remove a server
	public Server remove(){
		Random random = new Random();
		return servers.remove(random.nextInt(servers.size()));
	}
	
	@Override
	public String toString(){
		String result = "";
		int count = 0;
		for(Server server : servers){
			result += "{"+server.ip + "," + server.port + "," + server.from + "," + server.to+"}";
			if(++count < servers.size())
				result += ",";
		}
		return result;
	}
}
