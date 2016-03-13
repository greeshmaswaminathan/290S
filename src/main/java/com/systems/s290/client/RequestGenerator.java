package com.systems.s290.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestGenerator {
	
	private List<Long> userIds = new ArrayList<>();
	private Random randomizer = new Random();
	ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();
	static final Logger LOG = LoggerFactory.getLogger(RequestGenerator.class);
	static final Logger statichashLogger = LoggerFactory.getLogger("static"); 
	static final Logger consistenthashLogger = LoggerFactory.getLogger("consistent"); 
	static final long initialTime = System.currentTimeMillis();
	
	public RequestGenerator(){
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){

			@Override
			public void run() {
				newCachedThreadPool.shutdown();
				
			}
			
		}));
	}
	
	private void readUserIds() throws IOException{
		
		try(BufferedReader userIdReader = new BufferedReader(new FileReader(new File("resources/userIds")))){
			String userId = null;
			while((userId = userIdReader.readLine()) != null){
				userIds.add(Long.parseLong(userId));
			}
		}
		
		
	}
	
	private Long getRandomUserId(){
		return userIds.get(randomizer.nextInt(userIds.size()));
	}

	
	public void fireRandomRequest() throws IOException, SQLException, InterruptedException, ExecutionException{
		LOG.info("Starting request firing");
		readUserIds();
		final RequestHandler handler = new RequestHandler();
		long startTime = System.nanoTime();
		long endTime = startTime + TimeUnit.NANOSECONDS.convert(15L, TimeUnit.MINUTES);
		long addTime = startTime + TimeUnit.NANOSECONDS.convert(1L, TimeUnit.MINUTES);
		long removeTime = startTime + TimeUnit.NANOSECONDS.convert(11L, TimeUnit.MINUTES);
		boolean serverAdded = false;
		boolean serverRemoved = false;
		Future<?> future = null;
		while(System.nanoTime() < endTime){
			//Run for some time
			Long randomUserId = getRandomUserId();
			//LOG.info("Requesting for user:"+randomUserId);
			newCachedThreadPool.submit(new RequestUser(randomUserId,handler));
			
			//Initiate an addition
			if(System.nanoTime() > addTime && !serverAdded){
				ExecutorService newSingleThreadExecutor = Executors.newSingleThreadExecutor();
				future = newSingleThreadExecutor.submit(new Runnable(){

					@Override
					public void run() {
						LOG.info("Adding server");
						consistenthashLogger.info("Adding server");
						statichashLogger.info("Adding server");
						try {
							handler.addServer();
						} catch (SQLException e) {
							LOG.info("Exception in adding server",e);
						}
						
						LOG.info("Adding server completed");
						consistenthashLogger.info("Adding server completed");
						statichashLogger.info("Adding server completed");
						
					}});
				
				serverAdded = true;
				
			}
			//Initiate a removal
			else if((System.nanoTime()) > removeTime && !serverRemoved && future.get() == null){
				new Thread(new Runnable(){

					@Override
					public void run() {
						LOG.info("Removing server");
						consistenthashLogger.info("Removing server");
						statichashLogger.info("Removing server");
						try {
							handler.removeServer();
						} catch (SQLException e) {
							LOG.info("Exception in removing server",e);
						}
						LOG.info("Removing server completed");
						consistenthashLogger.info("Removing server completed");
						statichashLogger.info("Removing server completed");
					}}).start();
				serverRemoved = true;
				
			}
			else{
				Thread.sleep(250);
			}
		}
		
		
		
	}
	
	public static void main(String[] args) throws IOException, SQLException, InterruptedException, ExecutionException {
		new RequestGenerator().fireRandomRequest();
	}
}

class RequestUser implements Runnable{
 
	private long userId;
	private RequestHandler handler;
	
	
	public RequestUser(long userId, RequestHandler handler){
		this.userId = userId;
		this.handler = handler;
	}
	
	@Override
	public void run() {
		long startTime = System.nanoTime();
		handler.getTweetsFromUser(userId+"",RequestHandler.CONSISTENT );
		RequestGenerator.consistenthashLogger.info((System.nanoTime() - startTime)+"");
		
		//startTime = System.nanoTime();
		//handler.getTweetsFromUser(userId+"",RequestHandler.STATIC );
		//RequestGenerator.statichashLogger.info((System.nanoTime() - startTime)+"");
	}
	
}