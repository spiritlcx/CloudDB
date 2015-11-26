package app_KvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.log4j.*;
/**
 * Persistence class to handle the storage of data on the disk.
 */
public class Persistance {
	private File file;
	private BufferedReader reader;
	private BufferedWriter writer;
	public Persistance(String ip, int port){
		try {
			file = new File("persisteddata"+ip+port);
			file.createNewFile();
		} catch (IOException e) {
			Logger.getRootLogger().error("Persistance error.", e);
		}
	}
	/**
	 * Stores a key-value-pair in the storage file.
	 * @param key The key to store
	 * @param value The value to store
	 */
	public void store(String key, String value){
		try {
			writer = new BufferedWriter(new FileWriter(file, true));
			
			if(lookup(key) != null){
				remove(key);
			}
			writer.write(key + ',' + value + "\n");
			writer.flush();
			writer.close();
			writer = null;
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
	
	public String lookup(String key){
		String keyvalue;
		try {
				reader = new BufferedReader(new FileReader(file));
			
				while((keyvalue = reader.readLine())!=null){

					String [] kvpair = keyvalue.split(",", 2);
					if(kvpair != null && kvpair[0].equals(key)){
						reader.close();
						reader = null;
						return kvpair[1];
					}
				}
				reader.close();
				reader = null;
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
		String removeSuccess = "";
		try {
				reader = new BufferedReader(new FileReader(file));
				
				File tempFile = new File("persisteddata.tmp");
				BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile, true));
				String line;

				while((line = reader.readLine()) != null) {

					if(!line.trim().split(",")[0].equals(key)) {
						tempWriter.write(line + "\n");
						tempWriter.flush();
					}
				}
				tempWriter.close();
				reader.close();
				reader = null;
				
				if (!file.delete()) {
					removeSuccess = "File could not be deleted.";
				}
				else{
					if (!tempFile.renameTo(file))	{
						removeSuccess = "File could not be renamed.";
					}
					else{
						removeSuccess = "Key: " + key + " succesfully deleted!";
					}
				}

		    }
		    catch (FileNotFoundException e) {
		    	Logger.getRootLogger().error("Data storage file not found.", e);
		    }
		    catch (IOException e) {
		    	Logger.getRootLogger().error("Data storage error.", e);
		    }
		
		return removeSuccess;
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
