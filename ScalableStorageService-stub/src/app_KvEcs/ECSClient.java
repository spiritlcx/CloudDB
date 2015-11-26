package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.TextMessage;
import ecs.ECS;

public class ECSClient {
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ECSClient> ";
	private BufferedReader stdin;
	private ECS ecs = null;
	private boolean stop = false;
	
	public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}
	
	/**
	 * The handleCommand method handles all possible commands entered into the
	 * command line. 	
	 * @param cmdLine The latest command line input
	 */
	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			if(ecs != null){
				ecs.shutDown();
			}
			System.out.println(PROMPT + "Application exit!");
		
		} 
		else if (tokens[0].equals("init")){
			if(tokens.length == 4) {
				ecs = new ECS();
				if(ecs != null){
					try {
						if(tokens[3].equals("FIFO") || tokens[3].equals("LRU") || tokens[3].equals("LFU")){
							ecs.startEcs(40000, Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), tokens[3]);
						}
						else
						{
							printError("<strategy> is not one out of FIFO | LFU | LRU!");
							logger.warn("Invalid <strategy> for command \"add\"");
						}
					} catch (NumberFormatException e) {
						printError("<numberOfNodes> or <cacheSize> is not a number!");
						logger.warn(e.getMessage());
					}						
				}
				else{
					printError("The ecs system was not initialized. Use \"init\" or \"help\".");
					logger.error("System not initialized.");
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} 
		else  if (tokens[0].equals("add")) {
			if(tokens.length == 3) {
				if(ecs != null){
					try {
						if(tokens[2].equals("FIFO") || tokens[2].equals("LRU") || tokens[2].equals("LFU")){
							ecs.addNode(Integer.parseInt(tokens[1]), tokens[2]);
						}
						else
						{
							printError("<strategy> is not one out of FIFO | LFU | LRU!");
							logger.warn("Invalid <strategy> for command \"add\"");
						}
					} catch (NumberFormatException e) {
						printError("<cacheSize> is not a number!");
						logger.warn(e.getMessage());
					}						
				}
				else{
					printError("The ecs system was not initialized. Use \"init\" or \"help\".");
					logger.error("System not initialized.");
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("start")){
			if(ecs != null){
				ecs.start();
			}else{
				printError("The ecs system was not initialized. Use \"init\" or \"help\".");
				logger.error("System not initialized.");
			}
			
		}else if(tokens[0].equals("stop")){
			if(ecs != null){
				ecs.stop();
			}else{
				printError("The ecs system was not initialized. Use \"init\" or \"help\".");
				logger.error("System not initialized.");
			}
			
		}else if(tokens[0].equals("shutDown")){
			if(ecs != null){
				ecs.shutDown();
			}else{
				printError("The ecs system was not initialized. Use \"init\" or \"help\".");
				logger.error("System not initialized.");
			}
			
		}else if(tokens[0].equals("remove")){
			if(ecs != null){
				ecs.removeNode();
			}else{
				printError("The ecs system was not initialized. Use \"init\" or \"help\".");
				logger.error("System not initialized.");
			}
			
		}else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}
		
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("init <numberOfNodes> <cacheSize> <strategy>");
		sb.append("\t initializes service with <numberOfNodes> machines\n");
		sb.append(PROMPT).append("start");
		sb.append("\t\t\t\t\t starts the storage service \n");
		sb.append(PROMPT).append("stop");
		sb.append("\t\t\t\t\t stops the service \n");
		sb.append(PROMPT).append("shutDown");
		sb.append("\t\t\t\t stops all server processes \n");
		sb.append(PROMPT).append("add <cacheSize> <strategy>");
		sb.append("\t\t\t adds a new storage node \n");
		sb.append(PROMPT).append("remove");
		sb.append("\t\t\t\t removes a random storage node \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program and terminates the service");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}
	/**
	 * Handles incoming messages on the ecs client. Prints msg
	 * after the PROMT String.
	 */
	public void handleNewMessage(TextMessage msg) {
		if(!stop) {
			System.out.print(PROMPT);
			System.out.println(msg.getMsg());
		}
	}
	
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
	
	 	
    /**
     * Main entry point for the ecs application. 
     * @param args 
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/ecs.log", Level.OFF);
			ECSClient app = new ECSClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
