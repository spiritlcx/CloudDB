package testing;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

import org.junit.After;
import org.junit.Test;

import app_kvServer.KVServer;
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
	
	public void setUp() {
		int numnode = 7;
		int cacheSize = 10;
		ecs = new ECS();
		ecs.startEcs(40000, numnode, cacheSize, "FIFO");

		ecs.start();
	}

	
	@Test
	public void testRemoveNode(){
		Exception e1 = null;
		int size = ecs.getServers().size();
		try{
			ecs.removeNode();
		}catch(Exception e){
			e1 = e;
		}
		assert(e1 == null && ecs.getServers().size() == size-1);
	}
	
	@Test
	public void testAddNode(){
		Exception e1 = null;
		int size = ecs.getServers().size();
		try{
			ecs.addNode(3, "FIFO");
		}catch(Exception e){
			e1 = e;
		}
		assert(e1 == null && ecs.getServers().size() == size+1);
		
	}
	
	@Test
	public void testHashing(){
		ConsistentHashing hashing;
		String hashedKey = null;
		hashedKey = ConsistentHashing.getHashedKey("Test");
		
		assertTrue(hashedKey.equals("0cbc6611f5540bd0809a388dc95a615b"));
	}

	
	@Test
	public void testUpdateMeta(){
		Exception e1 = null;
		int size = ecs.getMetaData().size();
		try{
			ecs.addNode(3, "FIFO");
		}catch(Exception e){
			e1 = e;
		}
		assert(e1 == null && ecs.getMetaData().size() == size + 1);

	}
	
	@Test
	public void testStop(){
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
		System.out.println(message.getStatus());
		assertTrue(message != null && message.getStatus().equals(StatusType.SERVER_STOPPED));
	}
	@After
	public void clean(){
		ecs.shutDown();
	}
	
}