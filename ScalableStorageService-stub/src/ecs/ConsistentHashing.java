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

	private static MessageDigest md;

	public static String getHashedKey(String key){
		try {
			md  = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
