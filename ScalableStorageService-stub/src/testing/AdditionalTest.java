package testing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

import org.junit.Test;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import client.KVStore;

import ecs.ConsistentHashing;
import ecs.ECS;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	private ECS ecs;
	
	// TODO add your test cases, at least 3
	
	//Test ECS, consistent hashing, locks, metadata-update and retry operations
		
	//just needs to reach the end
	@Test
	public void testECS(){
		ecs = new ECS();
		ecs.startEcs(40000, 5, 5, "FIFO");
		ecs.addNode(5, "FIFO");
		ecs.removeNode();
		ecs.shutDown();
		assertTrue(true);
	}
	
		@Test
		public void testHashing(){
			ConsistentHashing hashing;
			String hashedKey = null;
			try {
				hashing = new ConsistentHashing();
				hashedKey = hashing.getHashedKey("Test");

			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			assertTrue(hashedKey.equals("0cbc6611f5540bd0809a388dc95a615b"));
			
		}
		
		@Test
		public void testLock(){
			ecs = new ECS();
			ecs.startEcs(40000, 5, 5, "FIFO");
			ecs.stop();
			
			KVStore client = new KVStore("127.0.0.1", 50000);
			try {
				client.connect();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			KVMessage message = null;
			try {
				message = client.put("key", "value");

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			assertTrue(message != null && message.getStatus().equals(StatusType.SERVER_STOPPED));
			
		}
}
