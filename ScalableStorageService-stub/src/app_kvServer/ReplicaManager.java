package app_kvServer;

import java.util.Iterator;
import java.util.TreeSet;

import store.StorageManager;

public class ReplicaManager {
//	private StorageManager storageManager;
	private Timestamp valueTimestamp;
	private Timestamp replicaTimestamp;
	private Timestamp [] tableTimestamp;
	private int identifier;
	private Log log;
	
	public ReplicaManager(int identifier, int size){
		this.identifier = identifier;
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
		message.replicaTimestmap = replicaTimestamp;
		return message;
	}
	
	public void query(Operation operation){
		if(operation.prev.compare(valueTimestamp) < 0){
//			String value = storageManager.get(operation.key);
		}
	}

	public Timestamp receiveUpdate(Operation operation){
		//update timestamp by plus 1 in this replicaManager position in the vector
		Timestamp ts = new Timestamp(operation.prev);
		ts.update(identifier);
		replicaTimestamp.update(ts, identifier);
		
		Log.Record record = log.new Record(identifier, ts, operation, operation.prev);
		log.add(record);
		update();
		return ts;
	}
	
	public void receiveGossip(GossipMessage gossipMessage){
		
		tableTimestamp[gossipMessage.identifier] = gossipMessage.replicaTimestmap;
		
		Log gossipLog = gossipMessage.replicaLog;
		TreeSet<Log.Record> gossipRecords = gossipLog.getRecords();
		
		Iterator<Log.Record> it = gossipRecords.iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			if(!(record.ts.compare(replicaTimestamp) < 0)){
				log.add(record);
			}
		}

		replicaTimestamp.merge(gossipMessage.replicaTimestmap);

		update();
		
		eliminate();
	}

	private void update(){
		Iterator<Log.Record> it = log.getRecords().iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			
			if(record.uprev.compare(valueTimestamp) < 0){
				Operation operation = record.uoperation;
				System.out.println(operation.key + " is inserted");
//				storageManager.put(operation.key, operation.value);
				valueTimestamp.merge(record.ts);
			}else{
				return;
			}
		}
	}
	
	//eliminate records in the log
	private void eliminate(){
		Iterator<Log.Record> it = log.getRecords().iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			boolean flag = true;
			for(Timestamp timestamp : tableTimestamp){
				if(!(record.ts.compare(timestamp) < 0))
					flag = false;
			}
			if(flag)
				it.remove();
		}
	}

	public static void main(String [] args){
		ReplicaManager r1 = new ReplicaManager(0, 3);
		ReplicaManager r2 = new ReplicaManager(1, 3);
		ReplicaManager r3 = new ReplicaManager(2, 3);

		Operation update = new Operation();
		update.key = "aa";
		update.value = "bb";
		update.prev = new Timestamp(3);
		Timestamp ts = r1.receiveUpdate(update);
		
		Operation update1 = new Operation();
		update1.key = "bb";
		update1.value = "bb";
		update1.prev = ts;
		
		ts = r2.receiveUpdate(update1);
		r2.receiveGossip(r1.getGossip());

		Operation update2 = new Operation();
		update2.key = "cc";
		update2.value = "bb";
		update2.prev = ts;

		r3.receiveUpdate(update2);
//		r3.receiveGossip(r2.getGossip());
		r3.receiveGossip(r1.getGossip());
		
	}
}

class GossipMessage{
	int identifier;
	Timestamp replicaTimestmap;
	Log replicaLog;
}

class Operation{
	String key;
	String value;
	Timestamp prev;
}

class Log{

	private TreeSet<Record> records = new TreeSet<Record>();
	
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
		public Record(int identifier, Timestamp ts, Operation uoperation, Timestamp uprev){
			this.identifier = identifier;
			this.ts = ts;
			this.uoperation = uoperation;
			this.uprev = uprev;
		}
		int identifier;
		Timestamp ts;
		Operation uoperation;
		Timestamp uprev;
		Id uid;
		@Override
		public int compareTo(Object o) {
			Timestamp other = ((Record)o).uprev;
			return uprev.compare(other);
		}
		
	}
}

class Timestamp{
	private int [] vector;
	public Timestamp(int n){
		vector = new int[n];
	}
	public Timestamp(Timestamp timestamp){
		vector = new int[timestamp.length()];
		int [] othervector = timestamp.getVector();
		for(int i = 0; i < vector.length; i++){
			vector[i] = othervector[i];
		}
	}
	public void merge(Timestamp timestamp){
		if(vector.length != timestamp.length())
			return;
		int [] othervector = timestamp.getVector();
		for(int i = 0; i < timestamp.length(); i++){
			if(vector[i] < othervector[i])
				vector[i] = othervector[i];
		}
	}
	
	public void update(int i){
		if(i >= vector.length)
			return;
		vector[i]++;
	}
	
	public void update(Timestamp timestamp, int i){
		if(vector.length != timestamp.length())
			return;
		int [] othervector = timestamp.getVector();
		vector[i] = othervector[i];
	}
	
	public int compare(Timestamp timestamp){
		int [] othervector = timestamp.getVector();
		if(vector.length != othervector.length)
			return 0;
		boolean smaller = false;
		boolean bigger = false;
		boolean equal = false;
		for(int i = 0; i < vector.length; i++){
			if(vector[i] < othervector[i]){
				smaller = true;
			}else if(vector[i] > othervector[i]){
				bigger = true;
			}else{
				equal = true;
			}
		}

		if((smaller || equal) && !bigger){
			return -1;
		}else if(bigger && !smaller){
			return 1;
		}else{
			return 0;
		}
	}
	
	public int length(){
		return vector.length;
	}
	public int [] getVector(){
		return vector;
	}
	
	@Override
	public String toString(){
		String s = "";
		for(int v : vector){
			s += (v + " ");
		}
		return s;
	}
}