package ecs;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

/**
 * The class will take a few servers with ip and port
 * with method distribute, it will calculate hash values for all servers, sorting and returning
 * @author spiritlcx
 *
 */

public class ConsistentHashing {
	private ArrayList<Server> servers = new ArrayList<Server>();
	private MessageDigest md;

	public ConsistentHashing() throws NoSuchAlgorithmException{
		md  = MessageDigest.getInstance("MD5");
	}
	
	public void add(Server server){
		servers.add(server);
	}
	
	public String getHashedKey(String ip, int port){
		return getHashedKey(ip+port);
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
	
	public ArrayList<Server> distribute(){
		ArrayList<Server> hashedservers = new ArrayList<Server>();

		for(Server server : servers){
			md.reset();
			md.update((server.ip + server.port).getBytes());
			byte[] result = md.digest();
			String hashedkey = "";
			for(int i = 0; i < 16; i++){
				int n = result[i] >> 4 & 0x0F;
			 	hashedkey += String.format("%1s", Integer.toHexString(n));
			 	n = result[i] & 0xF;
			 	hashedkey += String.format("%1s", Integer.toHexString(n));
			 }
			server.hashedkey = hashedkey;
			hashedservers.add(server);
		}

		hashedservers.sort(new Server());
		return hashedservers;
	}
}
