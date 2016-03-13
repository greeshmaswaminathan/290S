package com.systems.s290.data;

import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.ConsistentHash;
import com.systems.s290.db.DBHelper;

public class DistributedDirectoryStrategy implements HashingStrategy {

	static final Logger LOG = LoggerFactory.getLogger(HashingStrategy.class);
	private ConsistentHash<String> consistentHash;
	private SystemDetails systemDetails;
	
	public DistributedDirectoryStrategy(SystemDetails systemDetails) 
	{
		consistentHash = new ConsistentHash<String>(systemDetails.getServerCount(), systemDetails.getDistributedDirConnStrings());
		this.systemDetails = systemDetails;
	}
	
	

	public int getHash(Long primaryKeyValue) {
		String bin = consistentHash.getBinFor(primaryKeyValue);
		return systemDetails.getDistributedDirConnStrings().indexOf(bin);
	}

	@Override
	public String getTargetTableName() 
	{
		return "TweetsD";
	}

	@Override
	public String getDistributedDirTableName() {
		return "DistributedUserHash";
	}

	@Override
	public int getServerIndex(TwitterStatus status, List<String> serverConnectionStrings) {
		Long userId = status.getUserId();
		return getServerIndex(userId, serverConnectionStrings);
	}

	@Override
	public int getServerIndex(Long primaryKeyValue, List<String> targetConnectionDetails) {
		String dhtServer = consistentHash.getBinFor(primaryKeyValue);
		try {
			String targetServer = DBHelper.getServerForUserId(dhtServer, primaryKeyValue.longValue());
			return targetConnectionDetails.indexOf(targetServer);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}


}
