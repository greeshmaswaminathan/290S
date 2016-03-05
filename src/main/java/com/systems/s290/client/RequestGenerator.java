package com.systems.s290.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestGenerator {
	
	private List<Long> userIds = new ArrayList<>();
	private Random randomizer = new Random();
	ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(100);
	
	
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

	
	public void fireRandomRequest() throws IOException{
		readUserIds();
		long startTime = System.currentTimeMillis();
		long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(3L, TimeUnit.HOURS);
		long addTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(1L, TimeUnit.HOURS);
		long removeTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(2L, TimeUnit.HOURS);
		while(System.nanoTime() < endTime){
			//Run for some time
			newFixedThreadPool.submit(new RequestUser(getRandomUserId()));
			//Initiate an addition
			if((System.currentTimeMillis() - startTime) == addTime){
				//add a server
			}
			//Initiate a removal
			if((System.currentTimeMillis() - startTime) == removeTime){
				//remove server
			}
		}
		
		
		
	}
}

class RequestUser implements Runnable{
 
	private long userId;
	Logger statichashLogger = LoggerFactory.getLogger("static"); 
	Logger consistenthashLogger = LoggerFactory.getLogger("consistent"); 
	
	public RequestUser(long userId){
		this.userId = userId;
	}
	
	@Override
	public void run() {
		long startTime = System.nanoTime();
		Proxy.getTweetsFromUser(userId,Proxy.CONSISTENT );
		consistenthashLogger.info("Time taken for getting details from userId "+userId+" :"+(System.nanoTime() - startTime));
		startTime = System.nanoTime();
		Proxy.getTweetsFromUser(userId,Proxy.STATIC );
		statichashLogger.info("Time taken for getting details from userId "+userId+" :"+(System.nanoTime() - startTime));
	}
	
}