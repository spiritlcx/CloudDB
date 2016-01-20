package store;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.*;
/**
 * Persistence class to handle the storage of data on the disk.
 */
public class Persistance extends Storage{
	private File file;
	private BufferedReader reader;
	private BufferedWriter writer;
	public Persistance(String name){
		try {
			file = new File("persisteddata"+name);
			file.createNewFile();
		} catch (IOException e) {
			Logger.getRootLogger().error("Persistance error.", e);
		}
	}
	/**
	 * Stores a key-value-pair in the storage file.
	 * @param key The key to put
	 * @param value The value to put
	 */
	public void put(String key, String value){
		try {
			writer = new BufferedWriter(new FileWriter(file, true));
			
			if(get(key) != null){
				remove(key);
			}
			writer.write(key + ',' + value + "\n");
			writer.flush();
			writer.close();
		} catch (Exception e) {
			Logger.getRootLogger().error("Data storage error.", e);
		}
	}
	/**
	 * Searches for a key in the storage file.
	 * @param key The key to be looked for.
	 * @return The value associated with this key.
	 */
	
	public String read(){
		String keyvalue;
		try {
				reader = new BufferedReader(new FileReader(file));
			
				if((keyvalue = reader.readLine())!=null){

					String [] kvpair = keyvalue.split(",", 2);
					if(kvpair != null){
						reader.close();
						reader = null;
						return kvpair[0];
					}
				}
				reader.close();
				reader = null;
		} catch (IOException e) {
			Logger.getRootLogger().error("Data lookup error.", e);
		} finally{
			
		}

		return null;
		
	}
	
	public ArrayList<KeyValue> getAllPairs(){
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		try {
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null){
				String [] kvpair = temp.split(",", 2);
				if(kvpair.length == 2){
					results.add(new KeyValue(kvpair[0], kvpair[1]));
				}
			}
		} catch (IOException e) {
			Logger.getRootLogger().error("Data lookup error.", e);
		} finally{
			try {
				reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Logger.getRootLogger().error("Reader close error.", e);				
			}			
		}
		
		return results;
	}

	public ArrayList<KeyValue> get(String from, String to){
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		for(KeyValue keyvalue : getAllPairs()){
			if(inRange(keyvalue.getKey(), from, to))
				results.add(new KeyValue(keyvalue.getKey(), keyvalue.getValue()));
		}
		return results;
	}

	public void remove(String from, String to){
		for(KeyValue keyvalue : getAllPairs()){
			if(inRange(keyvalue.getKey(), from, to))
				remove(keyvalue.getKey());
		}		
	}

	
	public String get(String key){
		String keyvalue;
		try {
			reader = new BufferedReader(new FileReader(file));
		
			while((keyvalue = reader.readLine())!=null){

				String [] kvpair = keyvalue.split(",", 2);
				if(kvpair != null && kvpair[0].equals(key)){
					reader.close();
					return kvpair[1];
				}
			}
			reader.close();
		} catch (IOException e) {
			Logger.getRootLogger().error("Data lookup error.", e);
		} 

		return null;
	}
	/**
	 * Removes a data record with a certain key from the storage file.
	 * @param key The key to be looked for.
	 * @return A String documenting the results.
	 */
	public String remove(String key){
		String value = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			
			File tempFile = new File("persisteddata.tmp");
			BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile, true));
			String line;

			while((line = reader.readLine()) != null) {

				if(!line.trim().split(",")[0].equals(key)) {
					tempWriter.write(line + "\n");
					tempWriter.flush();
					value = line.trim().split(",")[1];
				}
			}
			tempWriter.close();
			reader.close();
			
			file.delete();
			tempFile.renameTo(file);
			
	    }
	    catch (FileNotFoundException e) {
	    	Logger.getRootLogger().error("Data storage file not found.", e);
	    }
	    catch (IOException e) {
	    	Logger.getRootLogger().error("Data storage error.", e);
	    }
		return value;
	}
	
	/**
	 * Closes reader and writer. Probably not necessary anymore.
	 */
	public void close(){
		try {
			reader.close();
			writer.close();
		} catch (IOException e) {
			Logger.getRootLogger().warn("Data storage error.", e);
		}
	}
}