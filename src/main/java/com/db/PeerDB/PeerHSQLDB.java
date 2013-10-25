/**
 * NAME: 
 * 		PeerHSQLDB.java
 * 
 * PURPOSE: 
 * 		Database for Peer server/client.
 * 		There is one table in the database called 'PeerFiles'.
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 */

package com.db.PeerDB;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

public class PeerHSQLDB {

	/*
	 * Database for Peer server/client
	 * There is one table in the database called 'PeerFiles'.
	 */

	/** The logger. */
	private static Logger LOGGER = Logger.getLogger(PeerHSQLDB.class);

	 
	/**
	 * initialize the tables in peer database. 
	 */
	public static void initDB() {
		LOGGER.info("initializing PeerHSQLDB...");
		try {
			Class.forName("org.hsqldb.jdbcDriver");
			Connection conn = getConnection();
			String fileTable = "PeerFiles";
			String createFileTableSQL = "CREATE TABLE "+fileTable + " (" 
					+ "id         VARCHAR(200)    NOT NULL primary key,"
					+ "file_path         VARCHAR(200)                  NOT NULL," 
					+ "file_name       VARCHAR(200)     NOT NULL,"
					+ "file_size	INT      NOT NULL, " 
					+ "file_version		INT		NOT NULL, "
					+ "file_state		VARCHAR(200)		NOT NULL,   "
					+ "owner_ip			VARCHAR(200)		NOT NULL,	"
					+ "owner_ttr          INT       NOT NULL,"
					+ "last_modified  timestamp			NOT NULL,"	
					+ "constraint unique_file_and_file_path UNIQUE ( file_path,file_size) )";
			
			String messageTable = "Messages";
			String createMessageTableSQL = "CREATE TABLE "+messageTable + " (" 
					+ "message_id         VARCHAR(200)    NOT NULL primary key,"
					+ "upstream_ip         VARCHAR(200)    NOT NULL," 
					+ "upstream_port       VARCHAR(200)     NOT NULL,"
					+ "time_insert	timestamp      NOT NULL, "
					+ "time_expire	timestamp      NOT NULL ,  "
					+ "file_name VARCHAR(200)      NOT NULL)   ";
			
			try {
				if(!checkTableExists(conn, fileTable)) {
					Statement stat = conn.createStatement();
					stat.executeUpdate(createFileTableSQL);
					stat.close();
					LOGGER.info("Table " + fileTable + " creates successfully.");
				}
				
				if(!checkTableExists(conn, messageTable)) {
					Statement stat = conn.createStatement();
					stat.executeUpdate(createMessageTableSQL);
					stat.close();
					LOGGER.info("Table " + messageTable + " creates successfully.");
				}
				
			} catch (SQLException e) {
				LOGGER.error("initialization exception:",e);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		

			conn.close();
			LOGGER.info("initializing PeerHSQLDB Successfully!");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
	}
	
	/**
	 * get connection to the database
	 * 
	 * @return the connection
	 * @throws SQLException
	 *             the sQL exception
	 */
	public static Connection getConnection() throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:peerhsqldb/db", "SA", "");
		return conn;
	}
	
	/**
	 * check whether a table is exits
	 * 
	 * @param conn
	 *            the connection
	 * @param tableName
	 *            the table name
	 * @return true, if successful
	 */
	public static boolean checkTableExists (Connection conn, String tableName) {
		boolean checkTable = false;
		
		try {
			DatabaseMetaData metaData = conn.getMetaData();
			ResultSet resultSet = metaData.getTables(null, null, tableName,null);
			while(resultSet.next()) {
				checkTable = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return checkTable;
	}

}
