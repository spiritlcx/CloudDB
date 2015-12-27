package ecs;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The class will take a few servers with ip and port
 * with method distribute, it will calculate hash values for all servers, sorting and returning
 * @author spiritlcx
 *
 */

public class ConsistentHashing {
	private TreeMap<String, Server> servers = new TreeMap<String, Server>();
	private MessageDigest md;

	public ConsistentHashing() throws NoSuchAlgorithmException{
		md  = MessageDigest.getInstance("MD5");
	}
	
	public void add(Server server){
		server.hashedkey = getHashedKey(server.ip + server.port);
		servers.put(server.hashedkey, server);
	}
	
	public TreeMap getServers(){
		return servers;
	}
	
	public String getHashedKey(String key){
		md.reset();
		md.update((key).getBytes());
		byte[] result = md.digest();
		String hashedkey = "";
		for(int i = 0; i < 16; i++){
			int n = result[i] >> 4 & 0x0F;
		 	hashedkey += String.format("%1s", Integer.toHexString(n));
		 	n = result[i] & 0xF;
		 	hashedkey += String.format("%1s", Integer.toHexString(n));
		 }
		return hashedkey;		
	}
}
