package com.systems.s290.data;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.ConsistentHash;

public class ConsistentHashStrategy implements HashingStrategy, IConsistentHashStrategy {

	static final Logger LOG = LoggerFactory.getLogger(HashingStrategy.class);
	private ConsistentHash<String> consistentHash;
	//private SystemDetails systemDetails;
	
	public ConsistentHashStrategy(int serverCount, List<String> serverConnectionStrings ) {
		consistentHash = new ConsistentHash<String>(serverCount, serverConnectionStrings);
		//this.systemDetails = systemDetails;
	}
	

	public int getServerIndex(Long primaryKeyValue, List<String> targetConnectionDetails, Map<String, Object> extraInfo) {
		String bin = consistentHash.getBinFor(primaryKeyValue);
		return targetConnectionDetails.indexOf(bin);
	}

	public void removeBin(String serverToRemove){
		consistentHash.removeBin(serverToRemove);
	}
	
	public void addBin(String serverToAdd){
		consistentHash.removeBin(serverToAdd);
	}
	
	public int getHashForServer(String server){
		return consistentHash.getHash(server);
	}

	@Override
	public String getTargetTableName() {
		return "TweetsC";
	}

		@Override
	public String getDistributedDirTableName() {
		return "";
	}
	

	@Override
	public String getBinFor(long userId) {
		return consistentHash.getBinFor(userId);
	}

	

}
