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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.systems.s290.data.ConsistentHashStrategy;
import com.systems.s290.data.DistributedDirectoryStrategy;
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
	private DistributedDirectoryStrategy distStrategy = null; 
	
	public static String CONSISTENT = "consistent";
	public static String STATIC = "static";
	public static String DISTRIBUTED = "distributed";
	
	List<Long> hotUsers = Collections.synchronizedList(new ArrayList<Long>());
	List<Long> hotUsersBeingResolved = Collections.synchronizedList(new ArrayList<Long>());
	
	public RequestHandler()
	{
		List<String> targetConnectionStrings = readConfigFile("resources/serverconfig.txt");
		List<String> distconnectionStrings = readConfigFile("resources/dhtconfig");
		sysDetails = new SystemDetails();
		sysDetails.setSourceConnectionString(SOURCE_SERVER);
		sysDetails.setTargetConnectionStrings(targetConnectionStrings);
		sysDetails.setDistributedDirConnStrings(distconnectionStrings);
		consistentStrategy = new ConsistentHashStrategy(targetConnectionStrings.size(), targetConnectionStrings);
		staticStrategy = new StaticHashStrategy();
		distStrategy = new DistributedDirectoryStrategy(sysDetails);
	}

	private List<String> readConfigFile(String configFileName) {
		List<String> targetConnectionStrings = Collections.synchronizedList(new ArrayList<String>());
		try(BufferedReader reader = new BufferedReader(new FileReader(new File(configFileName))))
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
		return targetConnectionStrings;
	}
	
	public void getTweetsFromUser(String userId, String hashType) throws SQLException
	{
		long user = Long.parseLong(userId);
		HashMap<String, Object> emptyExtraInfo = new HashMap<String, Object>();
		if (hashType.equals(CONSISTENT))
		{
			guardedConsistentHashQuery();
			int bucket = consistentStrategy.getServerIndex(user, sysDetails.getTargetConnectionStrings(), emptyExtraInfo);
			requestUserInformation(sysDetails.getTargetConnectionStrings().get(bucket), consistentStrategy.getTargetTableName(), user);
		}
		else if(hashType.equals(STATIC))
		{
			guardedStaticHashQuery();
			int bucket = staticStrategy.getServerIndex(user, sysDetails.getTargetConnectionStrings(),emptyExtraInfo);
			requestUserInformation(sysDetails.getTargetConnectionStrings().get(bucket), staticStrategy.getTargetTableName(), user);
		}
		else if(hashType.equals(DISTRIBUTED))
		{
			HashMap<String, Object> extraInfo = new HashMap<String, Object>();
			extraInfo.put("hop","second");
			int bucket = distStrategy.getServerIndex(user, sysDetails.getTargetConnectionStrings(),extraInfo);
			extraInfo.clear();
			extraInfo.put("hop", "first");
			int firstHop = distStrategy.getServerIndex(user, sysDetails.getTargetConnectionStrings(), extraInfo);
			requestDistributedUserInformation(bucket, 
					firstHop, 
					distStrategy.getTargetTableName(), 
					user);
		}
	}
	
	
	private void requestDistributedUserInformation(int bucket,
			int firstHop, String targetTableName, long user) throws SQLException 
	{
		String currentTweetsServer = sysDetails.getTargetConnectionStrings().get(bucket);
		String disDirectoryServer = sysDetails.getDistributedDirConnStrings().get(firstHop);
		// If distributed directory has a hotspot
		if (!hotUsers.isEmpty() && hotUsers.contains(user))   
		{
			
			if(!hotUsersBeingResolved.contains(user)){
				hotUsersBeingResolved.add(user);
				LOG.info("Resolving hotspot for the user:"+user);
				// move around the users...
				// Get the user information for this user
				List<Long> tweetsToDelete = new ArrayList<>();
				List<TwitterStatus> tweetsToAdd = new ArrayList<>();
				
				String reqUSerString = "select * from main." + targetTableName + " where UserId = " + user;
				try(Connection conn = MySQLDataSource.getConnection(currentTweetsServer))
				{
					Statement stmt = conn.createStatement();
					try(ResultSet rs = stmt.executeQuery(reqUSerString))
					{
						while(rs.next()){
							tweetsToAdd.add(SplitTemplate.setTwitterStatusDetails(rs));
							tweetsToDelete.add(rs.getLong("TwitterStatusId"));
						}
					}
				}
				catch(SQLException e)
				{
					// log, and continue
					LOG.warn("error retrieving user info from table " + targetTableName, e);
					throw e;
				}
				
				int newBucket = getLightlyLoadedServer(bucket);
				String newTweetsServer = sysDetails.getTargetConnectionStrings().get(newBucket);
				insertTweets(tweetsToAdd, newTweetsServer,targetTableName);
				deleteTweets(tweetsToDelete, currentTweetsServer,targetTableName);	
				LOG.info("Deleting tweets for user:"+user+" from server "+currentTweetsServer);
				LOG.info("Inserting tweets for user:"+user+" to server "+newTweetsServer);
				// Update the location of the user's tweets in the distibuted Directory and the source server
				updateUserServerMapping(targetTableName, user, newTweetsServer, disDirectoryServer);
				hotUsers.remove(user);
				hotUsersBeingResolved.remove(user);
				synchronized(this){
					notifyAll();
				}
				LOG.info("Resolution for hotspot for the user:"+user+" completed");
			}
			else
			while (hotUsersBeingResolved.contains(user))
			{
				LOG.info("Waiting on hotspot being resolved for the user:"+user);
				waitWhileHotSpotResolves();
			}
			
			LOG.info("Finally trying to get tweets again for :"+user);
			getTweetsFromUser(user+"", DISTRIBUTED);
			
		}else{
			requestUserInformation(currentTweetsServer, targetTableName, user);
		}
		
	}

	
	private int getLightlyLoadedServer(int bucket) 
	{
		int count = sysDetails.getServerCount();
		int randomAdder = new Random().nextInt(count - 1) + 1;
		return (bucket + randomAdder) % count;
	}
	
	
	private synchronized void waitWhileHotSpotResolves() {
		try {
			wait();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void updateUserServerMapping(String targetTableName, long user,
			String newTweetsServer, String disDirectoryServer) {
		
		try(Connection conn = MySQLDataSource.getConnection(disDirectoryServer))
		{
			updateDistributedHashTable(conn, user, newTweetsServer);
			LOG.info("Updating DistributedUserHash in server "+disDirectoryServer+" with entry "+newTweetsServer);
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error retrieving user info from table " + targetTableName, e);
		}
		
		try(Connection conn = MySQLDataSource.getConnection(sysDetails.getSourceConnectionString()))
		{
			updateDistributedHashTable(conn, user, newTweetsServer);
			LOG.info("Updating DistributedUserHash in server "+sysDetails.getSourceConnectionString()+" with entry "+newTweetsServer);
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error retrieving user info from table " + targetTableName, e);
		}
	}

	private void updateDistributedHashTable(Connection conn, long user, String newTweetsServer)
			throws SQLException {
		
		String updateSql = "update main." + distStrategy.getDistributedDirTableName() + " set Server = ? where UserId = ?";
		try(PreparedStatement stmt = conn.prepareStatement(updateSql))
		{
			stmt.setString(1, newTweetsServer);
			stmt.setLong(2, user);
			stmt.executeUpdate();
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
		
		sysDetails.getTargetConnectionStrings().add(SERVER6);
		
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
		
		sysDetails.getTargetConnectionStrings().remove(serverToRemove);
		
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
	
	private void removeServerForConsistentHash(String serverToRemove) throws SQLException 
	{
		LOG.debug("Removing a server from the system for consistent hash");
		consistentStrategy.removeBin(serverToRemove);
		
		ArrayList<TwitterStatus> tweets = readTweetsFromServer(serverToRemove);
		HashMap<String, List<TwitterStatus>> tweetsToInsert = new HashMap<>();
		List<Long> tweetsToDelete = new ArrayList<>();
		for (TwitterStatus tw : tweets)
		{
			tweetsToDelete.add(tw.getTwitterStatusId());
			String newBin = consistentStrategy.getBinFor(tw.getUserId());
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
			insertTweets(tweetsToInsert.get(serverConn), serverConn,consistentStrategy.getTargetTableName());
		}
		
		
		//deleteTweets(tweetsToDelete, serverToRemove); - do it manually
	}

	private void addServerForConsistentHash() throws SQLException 
	{
		HashMap<Integer, String> connStringConsisMap = new HashMap<>();
		List<String> connStrings = sysDetails.getTargetConnectionStrings();
		for (String conn : connStrings)
		{
			for (int i = 0; i< 5; i++)
			{
				// for each virtual node of this new node
				int hash = consistentStrategy.getHashForServer(conn + i);
				connStringConsisMap.put(hash, conn+i);
			}
		}
		
		String newServer = SERVER6;
		consistentStrategy.addBin(newServer);
		
		for (int i = 0; i< 5; i++)
		{
			// for each virtual node of this new node
			int hashCode = consistentStrategy.getHashForServer(newServer + i);
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
				String currentBin = consistentStrategy.getBinFor(tw.getUserId());
				if (currentBin.equalsIgnoreCase(newServer))
				{
					tweetsToDelete.add(tw.getTwitterStatusId());
					tweetsToAdd.add(tw);
					LOG.debug("Adding tweet id " + tw.getTwitterStatusId() + " for user " + tw.getUserId() + " to server: " + newServer);
					LOG.debug("Deleting tweet id " + tw.getTwitterStatusId() + " for user " + tw.getUserId() + " from server: " + connString);
				}
			}
			
			deleteTweets(tweetsToDelete, connString,consistentStrategy.getTargetTableName());
			insertTweets(tweetsToAdd, newServer,consistentStrategy.getTargetTableName());
		}
		
	}

	private void insertTweets(List<TwitterStatus> tweetsToAdd, String newServer, String tableName) throws SQLException 
	{
		try(Connection conn = MySQLDataSource.getConnection(newServer))
		{
			LOG.debug("Adding tweets to server " + newServer + " , tweets count :"+tweetsToAdd.size());
			SplitTemplate.batchWrite(conn, tweetsToAdd, tableName);
		}
		catch(SQLException e)
		{
			// log, and continue
			LOG.warn("error retrieving user info from server " + newServer, e);
			throw e;
		}
	}

	private void deleteTweets(List<Long> tweetsToDelete, String connString, String tableName) throws SQLException 
	{
		LOG.debug("Deleting tweets from server " + connString + " , tweets count :"+tweetsToDelete.size());
		String sqlDelete = "delete from main."+tableName+" where TwitterStatusId = ?";
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
			throw e;
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
	
	public void notifyHotSpot(Long user)
	{
		hotUsers.add(user);
	}
	
	public List<Long> getDistributedDirHotUser()
	{
		return hotUsers;
	}
	
	public void removeDistributedDirHotUser(Long user)
	{
		hotUsers.remove(user);
	}
}
