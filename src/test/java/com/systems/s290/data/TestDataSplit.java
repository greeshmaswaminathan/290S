package com.systems.s290.data;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TestDataSplit  {

	
	@Test
	public void testDataSplit() throws SQLException{
		SystemDetails details = new SystemDetails();
		List<String> connectionStrings = Collections.synchronizedList(new ArrayList<String>());
		connectionStrings.add("instance290-1.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-2.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-3.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-4.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		connectionStrings.add("instance290-5.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		details.setConnectionStrings(connectionStrings);
		details.setSourceConnectionString("instance290-0.cqxovt941ynz.us-west-2.rds.amazonaws.com:3306");
		//HashingStrategy split = new ConsistentHashStrategy(details);
		//new SplitTemplate().recreate(split, details);
		HashingStrategy split1 = new StaticHashStrategy(details);
		new SplitTemplate().recreate(split1, details);
	}
	

}
