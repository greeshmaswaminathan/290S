package com.systems.s290.data;

import java.sql.SQLException;
import java.util.List;

import com.cloudera.util.consistenthash.ConsistentHash;
import com.systems.s290.db.DBHelper;



public class DistributedHashingStrategy implements HashingStrategy{

	private ConsistentHash<String> consistentHash = null;
	
	public DistributedHashingStrategy(List<String> dhtServerConnectionStrings) {
		consistentHash = new ConsistentHash<>(5, dhtServerConnectionStrings);
	}
	
	@Override
	public int getServerIndex(TwitterStatus status, List<String> serverConnectionStrings) {
		Long userId = status.getUserId();
		String dhtServer = consistentHash.getBinFor(userId);
		try {
			String targetServer = DBHelper.getServerForUserId(dhtServer, userId.longValue());
			return serverConnectionStrings.indexOf(targetServer);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

	@Override
	public String getTargetTableName() {
		return "TweetsD";
	}

	@Override
	public int getServerIndex(Long primaryKeyValue, List<String> targetConnectionDetails) {
		// TODO Auto-generated method stub
		return 0;
	}

}
