package testing;

import org.junit.Test;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	
	// TODO add your test cases, at least 3
	
	@Test
	public void testStub() {
		assertTrue(true);
	}
	
	@Test
	public void testDeleteUnexist() {
		String key = "un exist key";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}

	@Test
	public void testFIFO(){
		Exception ex = null;
		KVMessage response = null;
		try {
			kvClient.put("aa", "bb");
			kvClient.put("cc", "dd");
			kvClient.put("ee", "ff");
			kvClient.put("gg", "hh");
			response = kvClient.get("aa");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

}
