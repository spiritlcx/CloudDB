package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Persistance {
	private File file;
	private BufferedReader reader;
	private BufferedWriter writer;
	public Persistance(){
		try {
			file = new File("persisteddata");
			file.createNewFile();
			reader = new BufferedReader(new FileReader(file));
			writer = new BufferedWriter(new FileWriter(file, true));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void store(String key, String value){
		try {
			if(lookup(key) == null){
				writer.write(key + ',' + value + '\n');
				writer.flush();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String lookup(String key){
		String keyvalue;
		try {
			while((keyvalue = reader.readLine())!=null){

				String [] kvpair = keyvalue.split(",");
				if(kvpair != null && kvpair[0].equals(key))
					return kvpair[1];
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String remove(String key){
//		writer.
		return null;
	}
	
	public void close(){
		try {
			reader.close();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
