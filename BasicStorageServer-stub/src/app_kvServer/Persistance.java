package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
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
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		return null;
	}
	
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
		    catch (FileNotFoundException ex) {
		      ex.printStackTrace();
		    }
		    catch (IOException ex) {
		      ex.printStackTrace();
		    }
		
		return removeSuccess;
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
