package app_kvServer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import store.StorageManager;

public class ReplicaManager {
	private StorageManager storageManager;
	private Timestamp valueTimestamp;
	private Timestamp replicaTimestamp;
	private Timestamp [] tableTimestamp;
	private int identifier;
	private Log log;
	
	public void query(Operation operation){
		if(operation.prev.compare(valueTimestamp) < 0){
			String value = storageManager.get(operation.key);
		}
	}

	public void receiveUpdate(Update update){
		//update timestamp by plus 1 in this replicaManager position in the vector
		Timestamp ts = new Timestamp(update.prev);
		ts.update(identifier);
		replicaTimestamp.update(ts, identifier);

		Log.Record record = log.new Record(identifier, ts, update.operation, update.prev);
		log.add(record);
		update();
	}
	
	public void receiveGossip(GossipMessage gossipMessage){
		
		tableTimestamp[gossipMessage.identifier] = gossipMessage.replicaTimestmap;
		
		Log gossipLog = gossipMessage.replicaLog;
		TreeSet<Log.Record> gossipRecords = gossipLog.getRecords();
		
		Iterator<Log.Record> it = gossipRecords.iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			if(replicaTimestamp.compare(record.uprev) < 0){
				log.add(record);
			}
		}

		replicaTimestamp.merge(gossipMessage.replicaTimestmap);

		update();
		
		eliminate();
	}

	public void update(){
		Iterator<Log.Record> it = log.getRecords().iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			if(record.uprev.compare(valueTimestamp) < 0){
				Operation operation = record.uoperation;
				storageManager.put(operation.key, operation.value);
				valueTimestamp.merge(record.uprev);
			}else{
				return;
			}
		}
	}
	
	//eliminate records in the log
	public void eliminate(){
		Iterator<Log.Record> it = log.getRecords().iterator();
		while(it.hasNext()){
			Log.Record record = (Log.Record)it.next();
			boolean flag = true;
			for(Timestamp timestamp : tableTimestamp){
				if(record.ts.compare(timestamp) > 0)
					flag = false;
			}
			if(flag)
				it.remove();
		}
	}
	
	public void applyUpdate(){
		
	}
}

class GossipMessage{
	int identifier;
	Timestamp replicaTimestmap;
	Log replicaLog;
}

class Update{
	int operationId;
	Operation operation;
	Timestamp prev;
}

class Operation{
	enum Type{query, update};
	String key;
	String value;
	Timestamp prev;
	Type type;
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
			Timestamp other = (Timestamp)o;
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
}