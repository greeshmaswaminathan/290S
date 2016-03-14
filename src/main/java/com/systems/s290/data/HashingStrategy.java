package com.systems.s290.data;

import java.util.List;
import java.util.Map;

public interface HashingStrategy {
	
	//public int getServerIndex(TwitterStatus status, List<String> serverConnectionStrings);
	public String getTargetTableName();	
	public String getDistributedDirTableName();
	public int getServerIndex(Long primaryKeyValue, List<String> targetConnectionDetails, Map<String,Object> extraInfo) ;

}
