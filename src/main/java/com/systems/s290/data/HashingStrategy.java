package com.systems.s290.data;

import java.util.List;

public interface HashingStrategy {
	
	public int getServerIndex(TwitterStatus status, List<String> serverConnectionStrings);
	public String getTargetTableName();
}
