package com.systems.s290.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.ConsistentHash;
import com.systems.s290.data.ConsistentHashStrategy;
import com.systems.s290.data.SplitTemplate;
import com.systems.s290.data.StaticHashStrategy;
import com.systems.s290.data.SystemDetails;
import com.systems.s290.data.TwitterStatus;
import com.systems.s290.db.connection.MySQLDataSource;

public class RequestHandler 
{
	private static final String SERVER6 = "instance290-6.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306";
	private static final String SOURCE_SERVER = "instance290-0.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306";
	static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
	private SystemDetails sysDetails = null;
	private AtomicBoolean staticRehashing = new AtomicBoolean(false);
	private AtomicBoolean consistentReHashing = new AtomicBoolean(false);
	private ConsistentHashStrategy consistentStrategy = null; 
	private StaticHashStrategy staticStrategy = null; 
	
	public static String CONSISTENT = "consistent";
	public static String STATIC = "static";
	
	public RequestHandler()
	{
		List<String> targetConnectionStrings = Collections.synchronizedList(new ArrayList<String>());
		try(BufferedReader reader = new BufferedReader(new FileReader(new File("resources/serverconfig.txt"))))
		{
			if (reader != null)
			{	
				String serverName = null;
				while((serverName = reader.readLine()) != null)
				{
					targetConnectionStrings.add(serverName);
				}
			}
		}
		catch (IOException e)
		{
			LOG.error("Unable to read serverconfig file, cannot load server details", e );
		}
		
		sysDetails = new SystemDetails();
		sysDetails.setSourceConnectionString(SOURCE_SERVER);
		sysDetails.setTargetConnectionStrings(targetConnectionStrings);
		consistentStrategy = new ConsistentHashStrategy(targetConnectionStrings.size(), targetConnectionStrings);
		staticStrategy = new StaticHashStrategy();
	}
	
	public void getTweetsFromUser(String userId, String hashType)
	{
		long user = Long.parseLong(userId);
		if (hashType.equals(CONSISTENT))
		{
			guardedConsistentHashQuery();
			int bucket = consistentStrategy.getHash(user, sysDetails.getTargetConnectionStrings());
			requestUserInformation(sysDetails.getTargetConnectionStrings().get(bucket), consistentStrategy.getTargetTableName(), user);
		}
		else
		{
			guardedStaticHashQuery();
			int bucket = staticStrategy.getHash(user, sysDetails.getServerCount());
			requestUserInformation(sysDetails.getTargetConnectionStrings().get(bucket), staticStrategy.getTargetTableName(), user);
		}
	}
	
	private synchronized void guardedConsistentHashQuery(){
		
		while(consistentReHashing.get()){
			try {
	            wait();
	        } catch (InterruptedException e) {}
		}
	}
	
	private synchronized void guardedStaticHashQuery(){
		
		while(staticRehashing.get()){
			try {
	            wait();
	        } catch (InterruptedException e) {}
		}
	}

	
	private void requestUserInformation(String serverConnString, String tableName, long userId) 
	{
		String reqUSerString = "select * from main." + tableName + " where UserId = " + userId;
		try(Connection conn = MySQLDataSource.getConnection(serverConnString))
		{
			Statement stmt = conn.createStatement();
			stmt.executeQuery(reqUSerString);
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error retrieving user info from table " + tableName, e);
		}
		
	}
	
	public void addServer() throws SQLException
	{
		LOG.debug("Adding a server to the system");
		
		
		consistentReHashing.set(true);
		addServerForConsistentHash();
		notifyConsistentHashQuery();
		
		sysDetails.getConnectionStrings().add(SERVER6);
		
		//SplitTemplate split = new SplitTemplate();
		//staticRehashing.set(true);
		//split.recreate(staticStrategy, sysDetails);
		//notifyStaticHashQuery();
		
	}
	
	public void removeServer() throws SQLException
	{
		String serverToRemove = SERVER6; 
		LOG.debug("Removing a server from the system : " + serverToRemove);
		
		consistentReHashing.set(true);
		removeServerForConsistentHash(serverToRemove);
		notifyConsistentHashQuery();
		
		sysDetails.getConnectionStrings().remove(serverToRemove);
		
		//SplitTemplate split = new SplitTemplate();
		//staticRehashing.set(true);
		//split.recreate(staticStrategy, sysDetails);
		//notifyStaticHashQuery();
		
		
	}

	public synchronized void notifyConsistentHashQuery() {
		consistentReHashing.set(false);
	    notifyAll();
	}
	
	public synchronized void notifyStaticHashQuery(){
		staticRehashing.set(false);
	    notifyAll();
	}
	
	private void removeServerForConsistentHash(String serverToRemove) 
	{
		LOG.debug("Removing a server from the system for consistent hash");
		ConsistentHash<String> consisHash = consistentStrategy.getConsistentHash();
		consisHash.removeBin(serverToRemove);
		
		ArrayList<TwitterStatus> tweets = readTweetsFromServer(serverToRemove);
		HashMap<String, List<TwitterStatus>> tweetsToInsert = new HashMap<>();
		List<Long> tweetsToDelete = new ArrayList<>();
		for (TwitterStatus tw : tweets)
		{
			tweetsToDelete.add(tw.getTwitterStatusId());
			String newBin = consisHash.getBinFor(tw.getUserId());
			List<TwitterStatus> tweetsForServer = tweetsToInsert.get(newBin);
			if (tweetsForServer == null)
			{
				tweetsForServer = new ArrayList<>();
				tweetsToInsert.put(newBin, tweetsForServer);
			}
			tweetsForServer.add(tw);
			LOG.debug("Adding tweet id " + tw.getTwitterStatusId() + " for user " + tw.getUserId() + " to server: " + newBin);
		}
		for (String serverConn : tweetsToInsert.keySet())
		{
			insertTweets(tweetsToInsert.get(serverConn), serverConn);
		}
		
		
		//deleteTweets(tweetsToDelete, serverToRemove); - do it manually
	}

	private void addServerForConsistentHash() 
	{
		ConsistentHash<String> consisHash = consistentStrategy.getConsistentHash();
		HashMap<Integer, String> connStringConsisMap = new HashMap<>();
		List<String> connStrings = sysDetails.getConnectionStrings();
		for (String conn : connStrings)
		{
			for (int i = 0; i< 5; i++)
			{
				// for each virtual node of this new node
				int hash = consisHash.getHash(conn + i);
				connStringConsisMap.put(hash, conn+i);
			}
		}
		
		String newServer = SERVER6;
		consisHash.addBin(newServer);
		
		for (int i = 0; i< 5; i++)
		{
			// for each virtual node of this new node
			int hashCode = consisHash.getHash(newServer + i);
			int smallestKey = findLargestKeyLessThanCurrent(connStringConsisMap.keySet(), hashCode);
			String vServer = connStringConsisMap.get(smallestKey);
			
			// Get server from vServer by removing last character
			// Connect to this server and read all its ids
			// Find which server bucket it belongs to based on consistent hash
			// Add to new server and delete from old server
			
			ArrayList<Long> tweetsToDelete = new ArrayList<>();
			ArrayList<TwitterStatus> tweetsToAdd = new ArrayList<>();
			String connString = vServer.substring(0, vServer.length() -1); 
			ArrayList<TwitterStatus> tweets = readTweetsFromServer(connString);
			for (TwitterStatus tw : tweets)
			{
				String currentBin = consisHash.getBinFor(tw.getUserId());
				if (currentBin.equalsIgnoreCase(newServer))
				{
					tweetsToDelete.add(tw.getTwitterStatusId());
					tweetsToAdd.add(tw);
					LOG.debug("Adding tweet id " + tw.getTwitterStatusId() + " for user " + tw.getUserId() + " to server: " + newServer);
					LOG.debug("Deleting tweet id " + tw.getTwitterStatusId() + " for user " + tw.getUserId() + " from server: " + connString);
				}
			}
			
			deleteTweets(tweetsToDelete, connString);
			insertTweets(tweetsToAdd, newServer);
		}
		
	}

	private void insertTweets(List<TwitterStatus> tweetsToAdd, String newServer) 
	{
		try(Connection conn = MySQLDataSource.getConnection(newServer))
		{
			LOG.debug("Adding tweets to server " + newServer + " , tweets count :"+tweetsToAdd.size());
			SplitTemplate.batchWrite(conn, tweetsToAdd, "TweetsC");
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error retrieving user info from server " + newServer, e);
		}
	}

	private void deleteTweets(List<Long> tweetsToDelete, String connString) 
	{
		LOG.debug("Deleting tweets from server " + connString + " , tweets count :"+tweetsToDelete.size());
		String sqlDelete = "delete from main.TweetsC where TwitterStatusId = ?";
		try(Connection conn = MySQLDataSource.getConnection(connString);
				PreparedStatement stmt = conn.prepareStatement(sqlDelete))
		{
			for (long id : tweetsToDelete)
			{
				stmt.setLong(1, id);
				stmt.addBatch();
			}
			
			stmt.executeBatch();
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error deleting tweets from server " + connString, e);
		}
		
	}

	private ArrayList<TwitterStatus> readTweetsFromServer(String connString) 
	{
		ArrayList<TwitterStatus> tweets = new ArrayList<>();
		String sql = "select * from main.TweetsC";
		try(Connection conn = MySQLDataSource.getConnection(connString))
		{
			Statement stmt = conn.createStatement();
			try(ResultSet rs = stmt.executeQuery(sql))
			{
				while(rs.next()){
					tweets.add(SplitTemplate.setTwitterStatusDetails(rs));
				}
				
			}
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error retrieving user info from server " + connString, e);
		}
		
		return tweets;
	}

	private int findLargestKeyLessThanCurrent(Set<Integer> keySet, int hashCode) 
	{
		int maxValue = Integer.MIN_VALUE;
		for (int i : keySet)
		{
			if (i < hashCode && i > maxValue)
			{
				maxValue = i;
			}
		}
		LOG.debug("Max value for hashcode " + hashCode + " is " + maxValue);
		return maxValue;
	}
}
