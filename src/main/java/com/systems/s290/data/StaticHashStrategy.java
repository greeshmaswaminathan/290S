package com.systems.s290.data;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.consistenthash.HashFunction;
import com.cloudera.util.consistenthash.MD5HashFunction;

public class StaticHashStrategy implements HashingStrategy {


	static final Logger LOG = LoggerFactory.getLogger(HashingStrategy.class);

	private HashFunction hashFunction;
	
	
	public StaticHashStrategy() {
		this.hashFunction = new MD5HashFunction();
		
	}


	@Override
	public String getTargetTableName() {
		return "TweetsS";
	}

	private int getHash(long userId, int count) {
		int hash = hashFunction.hash(userId) % count;
		if(hash < 0){
			hash+=count;
		}
		return hash;
	}

	@Override
	public int getServerIndex(Long primaryKeyValue, List<String> targetConnectionDetails,Map<String, Object> extraInfo) {
		return getHash(primaryKeyValue.longValue(), targetConnectionDetails.size());
	}
	
	@Override
	public String getDistributedDirTableName() {
		return "";
	}

}
