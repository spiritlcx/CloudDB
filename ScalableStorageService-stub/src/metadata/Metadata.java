package metadata;

import java.util.ArrayList;
import java.util.Random;

class Server{
	String ip;
	String port;
	String from;
	String to;
	
	public Server(String ip, String port, String from, String to){
		this.ip = ip;
		this.port = port;
		this.from = from;
		this.to = to;
	}
	
}

public class Metadata {
	private ArrayList<Server> servers = new ArrayList<Server>();
	public void add(String ip, String port, String from, String to){
		servers.add(new Server(ip, port, from, to));
	}

	// randomly remove a server
	public Server remove(){
		Random random = new Random();
		return servers.remove(random.nextInt(servers.size()));
	}
	
	@Override
	public String toString(){
		String result = "";
		int count = 0;
		for(Server server : servers){
			result += "{"+server.ip + "," + server.port + "," + server.from + "," + server.to+"}";
			if(++count < servers.size())
				result += ",";
		}
		return result;
	}
}
