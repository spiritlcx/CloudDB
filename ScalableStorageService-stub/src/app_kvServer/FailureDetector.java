package app_kvServer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class FailureDetector extends Thread{
		
	private Random random;
	private Members members;
	private Member self;
	private DatagramSocket server;
	private boolean isstop;
	private boolean isupdating;
	
	public FailureDetector(String ip, int port) throws IOException{
		self = new Member(ip, port);
		members = new Members();
		members.add(self);
		random = new Random();
		server = new DatagramSocket(port);
		isstop = false;
		isupdating = false;
	}
	
	public void add(String ip, int port){
		members.add(ip, port);
	}
	
	public void remove(String ip, int port){
		members.remove(ip, port);
	}
	
	class T{
		private static final double gossip = 2000;
		private static final double fail = 12000;
		private static final double cleanup = 2*fail;
		private static final double unit = 1000;
	}
	
	class Member{
		public Member(String ip, int port){
			this.ip = ip;
			this.port = port;
		}
		public Member(String ip, int port, int counter, double time){
			this.ip = ip;
			this.port = port;
			this.counter = counter;
			this.time = time;
		}

		String ip;
		int port;
		int counter;
		double time;
	}
	class Members{
		private ArrayList<Member> members = new ArrayList<Member>();
		public ArrayList<Member> getMembers(){
			return members;
		}
		public void add(Member member){
			members.add(member);
		}
		public void add(String ip, int port, int counter, double time){
			members.add(new Member(ip, port, counter, time));
		}
		public void add(String ip, int port){
			members.add(new Member(ip, port));
		}
		public void remove(String ip, int port){
			Iterator<Member> it = members.iterator();
			while(it.hasNext()){
				Member member = it.next();
				if(member.ip.equals(ip) && member.port == port){
					it.remove();
				}
			}
		}
		
		public void update(Members newmembers){
			isupdating = true;
			for(Member newmember : newmembers.getMembers()){
				boolean flag = false;
				Iterator<Member> it = members.iterator();
				while(it.hasNext()){
					Member member = it.next();
					if(member.ip.equals(newmember.ip) && member.port == newmember.port){
						if(newmember.counter > member.counter){
							member.counter = newmember.counter;
							member.time = self.time;
						}else{
							if(self.time - member.time > T.fail / T.unit){
								InetAddress ipaddress;
								try {
									String content = member.ip + " " + member.port;
									ipaddress = InetAddress.getByName("127.0.0.1");
									DatagramPacket sendPacket = new DatagramPacket(content.getBytes(),content.getBytes().length, ipaddress, 20000);
									server.send(sendPacket);
									System.out.println(member.port + " has failed");

								} catch (UnknownHostException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							if(self.time - member.time > T.cleanup / T.unit){
								System.out.println(member.port + " has been cleaned");
								it.remove();
							}
						}
						flag =true;
						break;
					}
				}
				if(flag == false){
					members.add(new Member(newmember.ip, newmember.port, newmember.counter, self.time));
				}
			}
			isupdating = false;
		}
		@Override
		public String toString(){
			String content = "";
			for(int i = 0; i < members.size(); i++){
				Member member = members.get(i);
				content += (member.ip + " " + member.port + " " + member.counter + " " + member.time +":");
			}
			return content;
		}
		public Members deserialize(String content){
			Members newMembers = new Members();
			String [] membermsgs= content.split(":");
			for(String membermsg : membermsgs){
				String [] components = membermsg.split(" ");
				if(components.length == 4){
					newMembers.add(new Member(components[0], Integer.parseInt(components[1]),Integer.parseInt(components[2]),Double.parseDouble(components[3])));
				}
			}
			return newMembers;
		}
	}

	@Override
	public void run(){
		new Thread(){
			public void run(){
				try {
					while(!isstop){
						send();
						Thread.sleep((long) T.gossip);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
		while(!isstop){
			try {
//				System.out.println(self.port + ":" + receive());
				receive();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void send() throws IOException{		
		self.counter++;
		self.time += T.gossip / T.unit;
		
		int index = -1;
		ArrayList<Member> members = this.members.getMembers();

		if(members.size() == 1)
			return;
		
		while((index = random.nextInt(members.size())) != -1){
			String ip = members.get(index).ip;
			int port = members.get(index).port;
			if(ip.equals(self.ip) && port == self.port)
				continue;
			
			InetAddress ipaddress = InetAddress.getByName(ip);

			while(isupdating){
				
			}

			DatagramPacket sendPacket = new DatagramPacket(this.members.toString().getBytes(), 0, this.members.toString().getBytes().length, ipaddress, port);

//			System.out.println(this.members.toString());
			
			server.send(sendPacket);

			break;
		}
	}
	
	private String receive() throws IOException{
		byte[] receiveData = new byte[1024];
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		server.receive(receivePacket);
		Members newmembers = new Members();
		newmembers = newmembers.deserialize(new String(receiveData));
		update(newmembers);
		return newmembers.toString();
	}

	private void update(Members newmembers){
		members.update(newmembers);
	}
	
	public void terminate(){
		server.close();
		isstop = true;
	}
	
	public static void main(String [] args){
		try {
			FailureDetector failureDetector = new FailureDetector("127.0.0.1", 40000);
			failureDetector.add("127.0.0.1", 40001);
			failureDetector.add("127.0.0.1", 40002);
			failureDetector.add("127.0.0.1", 40003);
			failureDetector.add("127.0.0.1", 40004);
			failureDetector.add("127.0.0.1", 40005);
			
			FailureDetector failureDetector1 = new FailureDetector("127.0.0.1", 40001);
			failureDetector1.add("127.0.0.1", 40000);
			failureDetector1.add("127.0.0.1", 40002);
			failureDetector1.add("127.0.0.1", 40003);
			failureDetector1.add("127.0.0.1", 40004);
			failureDetector1.add("127.0.0.1", 40005);

			FailureDetector failureDetector2 = new FailureDetector("127.0.0.1", 40002);
			failureDetector2.add("127.0.0.1", 40000);
			failureDetector2.add("127.0.0.1", 40001);
			failureDetector2.add("127.0.0.1", 40003);
			failureDetector2.add("127.0.0.1", 40004);
			failureDetector2.add("127.0.0.1", 40005);

			FailureDetector failureDetector3 = new FailureDetector("127.0.0.1", 40003);
			failureDetector3.add("127.0.0.1", 40000);
			failureDetector3.add("127.0.0.1", 40001);
			failureDetector3.add("127.0.0.1", 40002);
			failureDetector3.add("127.0.0.1", 40004);
			failureDetector3.add("127.0.0.1", 40005);

			FailureDetector failureDetector4 = new FailureDetector("127.0.0.1", 40004);
			failureDetector4.add("127.0.0.1", 40000);
			failureDetector4.add("127.0.0.1", 40001);
			failureDetector4.add("127.0.0.1", 40002);
			failureDetector4.add("127.0.0.1", 40003);
			failureDetector4.add("127.0.0.1", 40005);

			FailureDetector failureDetector5 = new FailureDetector("127.0.0.1", 40005);
			failureDetector5.add("127.0.0.1", 40000);
			failureDetector5.add("127.0.0.1", 40001);
			failureDetector5.add("127.0.0.1", 40002);
			failureDetector5.add("127.0.0.1", 40003);
			failureDetector5.add("127.0.0.1", 40004);

			
			failureDetector.start();
			failureDetector1.start();
			failureDetector2.start();
			failureDetector3.start();
			failureDetector4.start();
			failureDetector5.start();

			try{
				Thread.sleep(10000);
				failureDetector2.terminate();
//				failureDetector1.terminate();
			}catch(Exception e){
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
