package ecs;

import java.util.Comparator;

public class Server implements Comparator<Server>{
	public String ip;
	public String port;
	public String hashedkey;
	public String from;
	public String to;
		
	public Server(String ip, String port){
		this.ip = ip;
		this.port = port;
	}
	
	public Server(String ip, String port, String hashedkey, String from, String to){
		this.ip = ip;
		this.port = port;
		this.hashedkey = hashedkey;
		this.from = from;
		this.to = to;
	}
	
	@Override
	public int compare(Server o1, Server o2) {
		// TODO Auto-generated method stub
		if(o1.ip.equals(o2.ip) && o1.port.equals(o2.port))
			return 0;
		return o1.hashedkey.compareTo(o2.hashedkey);
	}	
	@Override
	public String toString(){
		return "server:{"+ip +","+ port+"," + from+"," + to+"}";
	}
}
