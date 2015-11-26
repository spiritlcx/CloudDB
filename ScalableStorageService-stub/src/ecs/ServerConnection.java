package ecs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import common.messages.KVAdminMessage;
import common.messages.MessageHandler;
import metadata.Metadata;
import common.messages.KVAdminMessage.StatusType;

public class ServerConnection extends Thread{
	private MessageHandler messageHandler;
	private InputStream input;
	private OutputStream output;
	private int cacheSize;
	private String displacementStrategy;
	private Metadata metadata;
	private Logger logger;
	private ECS ecs;
	
	/**
	 * This class handles the connection to the server.
	 * @param ecs		
	 * @param input		Input stream of the connected kvserver socket.
	 * @param output	Output stream of the connected kvserver socket.
	 * @param cacheSize	cacheSize of the KVServer.
	 * @param displacementStrategy	displacementStrategy of the KVServer.
	 * @param metadata	Metadata of the ECS,
	 * @param logger	Logger of the ECS.
	 */
	public ServerConnection(ECS ecs, InputStream input,OutputStream output,int cacheSize, String displacementStrategy, Metadata metadata, Logger logger){
		this.ecs = ecs;
		this.input = input;
		this.output = output;
		this.cacheSize = cacheSize;
		this.displacementStrategy = displacementStrategy;
		this.metadata = metadata;
		this.logger = logger;
		messageHandler = new MessageHandler(input, output, logger);
	}
	
	/**
	 * Sends initialization message to the KVServer.
	 */
	@Override
	public void run(){
		KVAdminMessage initMessage = new KVAdminMessage();
		initMessage.setCacheSize(cacheSize);
		initMessage.setDisplacementStrategy(displacementStrategy);
		initMessage.setMetadata(metadata);
		initMessage.setStatusType(StatusType.INIT);

		KVAdminMessage startMessage = new KVAdminMessage();
		startMessage.setStatusType(StatusType.START);

		try {
			messageHandler.sendMessage(initMessage.serialize().getMsg());

			messageHandler.sendMessage(startMessage.serialize().getMsg());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void startServer(){
		KVAdminMessage writeLock = new KVAdminMessage();
		writeLock.setStatusType(KVAdminMessage.StatusType.STOP);

		try {
			messageHandler.sendMessage(writeLock.getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void stopServer(){
		KVAdminMessage writeLock = new KVAdminMessage();
		writeLock.setStatusType(KVAdminMessage.StatusType.STOP);

		try {
			messageHandler.sendMessage(writeLock.getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void shutDown(){
		KVAdminMessage writeLock = new KVAdminMessage();
		writeLock.setStatusType(KVAdminMessage.StatusType.SHUTDOWN);

		try {
			messageHandler.sendMessage(writeLock.getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	
	public void setWriteLock(){
		KVAdminMessage writeLock = new KVAdminMessage();
		writeLock.setStatusType(KVAdminMessage.StatusType.WRITELOCK);

		try {
			messageHandler.sendMessage(writeLock.serialize().getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void releaseWriteLock(){
		KVAdminMessage writeLock = new KVAdminMessage();
		writeLock.setStatusType(KVAdminMessage.StatusType.WRITELOCK);

		try {
			messageHandler.sendMessage(writeLock.serialize().getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void receiveData(){
		KVAdminMessage receive = new KVAdminMessage();
		receive.setStatusType(KVAdminMessage.StatusType.RECEIVE);

		try {
			messageHandler.sendMessage(receive.serialize().getMsg());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void moveData(String from, String to, String ip){
		try {


			KVAdminMessage moveMessage = new KVAdminMessage();
			moveMessage.setStatusType(StatusType.MOVE);
			moveMessage.setFrom(from);
			moveMessage.setTo(to);
			moveMessage.setIp(ip);
			messageHandler.sendMessage(moveMessage.serialize().getMsg());
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
