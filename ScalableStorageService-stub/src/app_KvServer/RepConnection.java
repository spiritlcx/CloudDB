package app_KvServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.MessageHandler;
import common.messages.TextMessage;
import strategy.Strategy;

public class RepConnection implements Runnable {
	private ServerSocket replicaSocket;
	private Logger logger;
	private StorageManager storageManager;
	
	
	public RepConnection(ServerSocket replicaSocket, StorageManager storageManager, Logger logger){
		this.replicaSocket = replicaSocket;
		this.storageManager = storageManager;
		this.logger = logger;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Socket server = null;
		try {
			while((server = replicaSocket.accept()) != null){
				final Socket newserver = server;
				new Thread(){
					public void run(){
						byte [] message = null;
						MessageHandler serverMsgHandler = null;
						try {
							serverMsgHandler = new MessageHandler(newserver.getInputStream(), newserver.getOutputStream(), logger);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							logger.error(e.getMessage());
						}
	
						try {
							while((message = serverMsgHandler.receiveMessage()) != null){							
								TextMessage receivedMessage = new TextMessage(message);
								receivedMessage = receivedMessage.deserialize();
								storageManager.put(receivedMessage.getKey(), receivedMessage.getValue());
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							logger.error(e.getMessage());
						}
					}
				}.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
}
