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
	private ArrayList<ServerHash> servers = new ArrayList<ServerHash>();
	private MessageDigest md;

	public ConsistentHashing() throws NoSuchAlgorithmException{
		md  = MessageDigest.getInstance("MD5");
	}
	
	public void add(ServerHash server){
		servers.add(server);
	}
	
	public ArrayList<ServerHash> distribute(){
		ArrayList<ServerHash> hashedservers = new ArrayList<ServerHash>();

		for(ServerHash server : servers){
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

		hashedservers.sort(new ServerHash());
		return hashedservers;
	}
}
