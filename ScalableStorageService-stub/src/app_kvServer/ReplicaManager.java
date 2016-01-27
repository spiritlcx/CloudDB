package app_kvServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import common.assist.Operation;
import common.assist.Timestamp;
import common.messages.MessageHandler;
import common.messages.TextMessage;
import common.messages.KVMessage.StatusType;
import ecs.Server;
import store.StorageManager;

class OperationHandler{
	Operation operation;
	MessageHandler messageHandler;
}

class T{
	static long gossip = 10000;
}

public class ReplicaManager extends Thread{
	private StorageManager storageManager;
	private Timestamp valueTimestamp;
	private Timestamp replicaTimestamp;
	private Timestamp [] tableTimestamp;
	private int identifier;
	private Log log;
	private Block block0 = new Block();
	private Block block1 = new Block();

	private int port;
	private Logger logger;
	private MessageHandler [] messageReceiver;
	private MessageHandler [] messageSender;
	private ArrayList<OperationHandler> updateQueue = new ArrayList<OperationHandler>();
	private ArrayList<OperationHandler> queryQueue = new ArrayList<OperationHandler>();

	private TreeSet<String> executedTable = new TreeSet<String>();
	
	private ServerSocket server;
	public void run(){

		//build server
		try {
			server = new ServerSocket(port-100-identifier*10);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		new Thread(){
			public void run(){
				try {
					Socket client = null;
					while((client = server.accept()) != null){
						if(messageReceiver[0] == null){
							messageReceiver[0] = new MessageHandler(client, logger);
							synchronized(block0){
								block0.notifyAll();
							}
						}else{
							messageReceiver[1] = new MessageHandler(client, logger);
							synchronized(block1){
								block1.notifyAll();
							}
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();

		//receive gossip message from other replica managers
		new Thread(){
			public void run(){
				try {
					while(!server.isClosed()){
						while(messageReceiver[0] == null){
							try {
								synchronized(block0){
									block0.wait();
								}
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						
						String content = new String(messageReceiver[0].receiveMessage());
						GossipMessage gossipMessage = GossipMessage.deserialize(content);
						receiveGossip(gossipMessage);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					messageReceiver[0] = null;
				}
			}
		}.start();
		
		new Thread(){
			public void run(){
				try {
					while(!server.isClosed()){
						while(messageReceiver[1] == null){
							try {
								synchronized(block1){
									block1.wait();
								}
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}

						String content = new String(messageReceiver[1].receiveMessage());
						GossipMessage gossipMessage = GossipMessage.deserialize(content);
						receiveGossip(gossipMessage);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					messageReceiver[1] = null;
				}
			}
		}.start();

		Random random = new Random();
		
		new Thread(){
			public void run(){
				while(!server.isClosed()){
					try {
						Thread.sleep(T.gossip);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

					int i =random.nextInt(2);
										
					if(messageSender[i] != null){
						try {
							messageSender[i].sendMessage(getGossip().serialize());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}.start();
	}

	public void refresh(){
		messageReceiver[0] = null;
		messageReceiver[1] = null;
	}
	
	//connect to server on other replica managers
	public void connect(Server server1, Server server2){
		
		new Thread(){
			public void run(){
				try {
					int port1= 0;
					int port2 = 0;
					if(identifier == 0){
						port1 = 1;
						port2 = 2;
					}else if(identifier == 1){
						port1 = 0;
						port2 = 2;
					}else{
						port1 = 0;
						port2 = 1;
					}
					Socket client1 = new Socket(server1.ip, Integer.parseInt(server1.port)-100-port1*10);
					Socket client2 = new Socket(server2.ip, Integer.parseInt(server2.port)-100-port2*10);
					
					messageSender[0] = new MessageHandler(client1, logger);
					messageSender[1] = new MessageHandler(client2, logger);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();


	}
	
	public void close(){
		try {
			server.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	public ReplicaManager(int identifier, int size){
//		tableTimestamp = new Timestamp[size];
//		this.identifier = identifier;
//		valueTimestamp = new Timestamp(size);
//		replicaTimestamp = new Timestamp(size);
//
//		for(int i = 0; i < size; i++){
//			tableTimestamp[i] = new Timestamp(size);
//		}
//		block = new Block();
//		log = new Log();
//
//	}
	
	public ReplicaManager(int port, StorageManager storageManager, int identifier, int size, Logger logger){
		this.identifier = identifier;
		this.storageManager = storageManager;
		this.port = port;
		this.logger = logger;
		
		messageReceiver = new  MessageHandler[2];
		messageSender = new MessageHandler[2];
		
		valueTimestamp = new Timestamp(size);
		replicaTimestamp = new Timestamp(size);
		tableTimestamp = new Timestamp[size];
		for(int i = 0; i < size; i++){
			tableTimestamp[i] = new Timestamp(size);
		}
		log = new Log();
	}
	
	public GossipMessage getGossip(){
		GossipMessage message = new GossipMessage();
		message.identifier = identifier;
		message.replicaLog = log;
		message.replicaTimestamp = replicaTimestamp;
		return message;
	}
	
	public void query(OperationHandler operationHandler){
		Operation operation = operationHandler.operation;

		if(operation.prev.compare(valueTimestamp) < 0){
			doquery(operationHandler);
		}else{
			queryQueue.add(operationHandler);
		}

	}

	private void doquery(OperationHandler operationHandler){
		Operation operation = operationHandler.operation;
		TextMessage sentMessage = new TextMessage();
		MessageHandler messageHandler = operationHandler.messageHandler;
		String value = storageManager.get(operation.key);

		if(value == null){
			sentMessage.setStatusType(StatusType.GET_ERROR);
		}else{
			sentMessage.setStatusType(StatusType.GET_SUCCESS);
			sentMessage.setValue(value);

		}
		sentMessage.setKey(operation.key);
		sentMessage.setPrev(valueTimestamp);
		try {
			messageHandler.sendMessage(sentMessage.serialize().getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Timestamp receiveUpdate(OperationHandler operationHandler){
		//update timestamp by plus 1 in this replicaManager position in the vector
		Operation operation = operationHandler.operation;
		MessageHandler messageHandler = operationHandler.messageHandler;
		
		Timestamp ts = new Timestamp(operation.prev);
		replicaTimestamp.update(identifier);
		ts.update(replicaTimestamp, identifier);
		
		Log.Record record = log.new Record(identifier, ts, operation, operation.prev, operation.sequence);
		log.add(record);

		TextMessage sentMessage = new TextMessage();
		if(record.uprev.compare(valueTimestamp) < 0){
			StatusType type = storageManager.put(operation.key, operation.value);
			valueTimestamp.merge(record.ts);			
			sentMessage.setStatusType(type);
			executedTable.add(operation.sequence);
		}else{
			sentMessage.setStatusType(StatusType.BLOCKED);
			updateQueue.add(operationHandler);
		}
		sentMessage.setKey(operation.key);
		sentMessage.setValue(operation.value);
		sentMessage.setPrev(valueTimestamp);

		try {
			messageHandler.sendMessage(sentMessage.serialize().getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ts;
	}
	
	public void receiveGossip(GossipMessage gossipMessage){
		
		tableTimestamp[gossipMessage.identifier] = gossipMessage.replicaTimestamp;
		
		Log gossipLog = gossipMessage.replicaLog;
		if(gossipLog == null)
			return;
		TreeSet<Log.Record> gossipRecords = gossipLog.getRecords();
		
		Iterator<Log.Record> it = gossipRecords.iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			if(!(record.ts.compare(replicaTimestamp) < 0) && !executedTable.contains(record.sequence)){
				log.add(record);
			}
		}

		replicaTimestamp.merge(gossipMessage.replicaTimestamp);

		update();
		
		eliminate();
	}

	private void update(){
		Iterator<Log.Record> it = log.getRecords().iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();

			if(record.uprev.compare(valueTimestamp) < 0 && !executedTable.contains(record.sequence)){
				Operation operation = record.uoperation;
				storageManager.put(operation.key, operation.value);
				executedTable.add(record.sequence);
				valueTimestamp.merge(record.ts);
			}
		}

//		Iterator<OperationHandler> itupdate = updateQueue.iterator();
//		for(OperationHandler operationHandler : updateQueue){
//			
//		}

		Iterator<OperationHandler> itquery = queryQueue.iterator();
		while(itquery.hasNext()){
			OperationHandler operationHandler = (OperationHandler)itquery.next();
			Operation operation = operationHandler.operation;
			if(operation.prev.compare(valueTimestamp) < 0){
				doquery(operationHandler);
			}
			itquery.remove();
		}
	}
	
	//eliminate records in the log
	private void eliminate(){
		Iterator<Log.Record> it = log.getRecords().iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			boolean flag = true;
			int i = 0;
			for(Timestamp timestamp : tableTimestamp){
				if(identifier == i )
					continue;
				if(!(record.ts.compare(timestamp) < 0)){
					flag = false;
				}
				i++;
			}
			if(flag){
				it.remove();
			}
		}
	}

//	public static void main(String [] args){
//		ReplicaManager r1 = new ReplicaManager(0, 3);
//		ReplicaManager r2 = new ReplicaManager(1, 3);
//		ReplicaManager r3 = new ReplicaManager(2, 3);
//
//		Operation update = new Operation();
//		update.key = "aa";
//		update.value = "bb";
//		update.prev = new Timestamp(3);
//		Timestamp ts = r1.receiveUpdate(update);
//		
//		Operation update1 = new Operation();
//		update1.key = "bb";
//		update1.value = "bb";
//		update1.prev = ts;
//		
//		ts = r2.receiveUpdate(update1);
//		r2.receiveGossip(r1.getGossip());
//
//		Operation update2 = new Operation();
//		update2.key = "cc";
//		update2.value = "bb";
//		update2.prev = ts;
//
//		r3.receiveUpdate(update2);
//		r3.receiveGossip(r2.getGossip());
////		r3.receiveGossip(r1.getGossip());		
////		System.out.println(r2.getGossip().serialize());
////		System.out.println(r2.getGossip().replicaLog.toString());
//		System.out.println(GossipMessage.deserialize(r2.getGossip().serialize()));
////		Log log = Log.deserialize(r2.getGossip().replicaLog.toString());
////		TreeSet<Log.Record> records = log.getRecords();
////		Iterator it = records.iterator();
////		while(it.hasNext()){
////			System.out.println("ff");
////			Log.Record record = (Log.Record)it.next();
////			System.out.println(record.identifier);
////			System.out.println(record.ts);
////			System.out.println(record.uoperation);
////			System.out.println(record.uprev);
////
////		}
//	}
}

class Block{
	
}

class GossipMessage{
	int identifier;
	Timestamp replicaTimestamp;
	Log replicaLog;
	
	public String serialize(){
		return toString();
	}

	@Override
	public String toString(){
		String s = "";
		s += ("id::"+identifier + ",replicaTimestamp::"+replicaTimestamp + ",log::"+replicaLog);
		s = s + '\r'+'\n';
		return s;
	}
	
	public static GossipMessage deserialize(String s){
		GossipMessage gossipMessage = new GossipMessage();
		String [] elements = s.split(",", 3);
		for(String element : elements){
			String [] contents = element.split("::");
			switch(contents[0]){
			case "id":
				gossipMessage.identifier = Integer.parseInt(contents[1]);
				break;
			case "replicaTimestamp":
				gossipMessage.replicaTimestamp = Timestamp.deserialize(contents[1]);
				break;
			case "log":
				if(contents.length == 2)
					gossipMessage.replicaLog = Log.deserialize(contents[1]);
				break;
			}
		}
		return gossipMessage;
	}
}

class Log{

	private TreeSet<Record> records = new TreeSet<Record>();
	
	public void setRecords(TreeSet<Record> records){
		this.records = records;
	}
	
	public void add(Record record){
		records.add(record);
	}
	
	public TreeSet<Record> getRecords(){
		return records;
	}
	
	class Id{
		String ip;
		int port;
		int id;
	}
	
	class Record implements Comparable{
		public Record(){
			
		}
		public Record(int identifier, Timestamp ts, Operation uoperation, Timestamp uprev, String sequence){
			this.identifier = identifier;
			this.ts = ts;
			this.uoperation = uoperation;
			this.uprev = uprev;
			this.sequence = sequence;
		}
		int identifier;
		Timestamp ts;
		Operation uoperation;
		Timestamp uprev;
		String sequence;
		@Override
		public int compareTo(Object o) {
			Timestamp other = ((Record)o).uprev;
			return uprev.compare(other);
		}

		@Override
		public String toString(){
			String s = "";
			s += ("id:,"+identifier+",:ts:,"+ts+",:uoperation:,"+uoperation+",:uprev:,"+uprev+",:sequence:,"+sequence);
			return s;
		}
	}
	
	@Override
	public String toString(){
		String s = "";
		Iterator<Record> it = records.iterator();
		while(it.hasNext()){
			Record record = (Record)it.next();
			s += ("{"+ record + "},,");
		}
		return s;
	}

	public static Log deserialize(String s){
		TreeSet<Record> records = new TreeSet<Record>();
		String [] rstrings = s.split(",,");
		Log log = new Log();

		for(String rstring : rstrings){
			rstring = rstring.substring(1, rstring.length()-1);
			String [] elements = rstring.split(",:");
			Record record = log.new Record();

			for(String element : elements){

				String [] pair = element.split(":,");
				switch(pair[0]){
				case "id":
					record.identifier = Integer.parseInt(pair[1]);
					break;
				case "ts":
					record.ts = Timestamp.deserialize(pair[1]);
					break;
				case "uoperation":
					record.uoperation = Operation.deserialize(pair[1]);
					break;
				case "uprev":
					record.uprev = Timestamp.deserialize(pair[1]);
					break;
				case "sequence":
					record.sequence = pair[1];
				}
			}
			records.add(record);
		}
	
		log.setRecords(records);
		return log;
	}
}