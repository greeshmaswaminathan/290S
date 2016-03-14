package com.systems.s290.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.systems.s290.db.connection.MySQLDataSource;

public class DBHelper {
	
	private static final String DHT_ServerSelect = "select Server from main.DistributedUserHash where UserId = ?";
	static final Logger LOG = LoggerFactory.getLogger(DBHelper.class);
	
	public static String getServerForUserId(String connectionString, long userId) throws SQLException{
		String server = null;
		
		try(Connection conn = MySQLDataSource.getConnection(connectionString);
			PreparedStatement statement = conn.prepareStatement(DHT_ServerSelect)){
			statement.setLong(1, userId);
			try(ResultSet rs = statement.executeQuery()){
				while(rs.next()){
					server = rs.getString(1);
				}
			}
		}catch (SQLException exception) {
			LOG.info("Exception while trying to read Server for UserId "+userId+" from "+connectionString);
			throw exception;
		}
		return server;
	}

}
