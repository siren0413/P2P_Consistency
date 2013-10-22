/**
 * NAME: 
 * 		PeerDAO.java
 * 
 * PURPOSE: 
 * 		To insert, delete and find file from Peer's database. Peer database has
 * 		one table named 'PeerFiles' to store the files.
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 */
package com.dao;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.cache.PeerInfo;
import com.cache.PeerMessage;
import com.db.PeerDB.PeerHSQLDB;
import com.util.ID_Generator;

public class PeerDAO {

	/**
	 * To insert, delete and find file from Peer's database. Peer database has
	 * one table named 'PeerFiles' to store the files.
	 */
	

	/** The conn. */
	Connection conn;
	
	/** The stmt. */
	PreparedStatement stmt;
	
	/** The result. */
	ResultSet result;
	
	/** The statement. */
	Statement statement;


	/** 
	 * Upload file as master server into table PeerFiles
	 */
	public boolean uploadFile(String filePath,String fileName, int fileSize, int fileVersion) throws SQLException{
		try {
			conn = PeerHSQLDB.getConnection();
			String id = ID_Generator.generateID();
			String sql = "insert into PeerFiles values (?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, id);
			stmt.setString(2, filePath);
			stmt.setString(3, fileName);
			stmt.setInt(4, fileSize);
			stmt.setInt(5, fileVersion);
			stmt.executeUpdate();

			
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}return true;
	}
	
	/**
	 * insert into 'PeerDownloadedFiles' table with file path,file name and file size
	 * 
	 * @param filePath
	 *            the file path
	 * @param fileName
	 *            the file name
	 * @param fileSize
	 *            the file size
	 * @return true, if successful
	 * @throws SQLException
	 *             the sQL exception
	 */
	public boolean insertFile(String filePath, String fileName, int fileSize, 
									int fileVersion,String fileState, String originServerIP, String originServerPort) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			String id = ID_Generator.generateID();
			String sql = "insert into PeerDownloadedFiles values (?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, id);
			stmt.setString(2, filePath);
			stmt.setString(3, fileName);
			stmt.setInt(4, fileSize);
			stmt.setInt(5, fileVersion);
			stmt.setString(6, fileState);
			stmt.setString(7, originServerIP);
			stmt.setString(8, originServerPort);
			stmt.executeUpdate();

			return true;
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public boolean updateFileVersion(String fileName) throws SQLException {
		
		if (!isModifiable(fileName))
			return false;
		
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select file_version from PeerFiles where file_name like '" + fileName + "'";
			result = statement.executeQuery(sql);
			String num = "";
			while (result.next()) {
				num = result.getString(1);
				System.out.println("version is : " + num);
			}
			Integer versionNumber = Integer.parseInt(num);
			sql = "UPDATE PeerFiles SET file_version='" + versionNumber++ + "' where file_name like '" + fileName + "'";
			statement.executeUpdate(sql);
			return true;
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	 
	/**
	 * delete a specific file from PeerFiles table
	 * @param fileName
	 *            the file name
	 * @return true, if successful
	 * @throws SQLException
	 *             the sQL exception
	 */
	public boolean deleteFile(String fileName) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "delete from PeerFiles where file_name like '" + fileName + "'";
			statement.executeUpdate(sql);

			return true;
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	 
	/**
	 * find the file path of a specific file from 'PeerFiles'
	 * 
	 * @param fileName
	 *            the file name
	 * @return the string
	 * @throws SQLException
	 *             the sQL exception
	 */
	public String findFile(String fileName, String tableName) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select file_path from "+ tableName +" where file_name like '" + fileName + "'";
			result = statement.executeQuery(sql);
			while (result.next()) {
				return result.getString(1);
			}
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	
	/**
	 *  check whether a file is in the database
	 * 
	 * @param fileName
	 *            the file name
	 * @return true, if successful
	 * @throws SQLException
	 *             the sQL exception
	 */
	public boolean checkFileAvailable(String fileName, String tableName) throws SQLException {

		if (findFile(fileName,tableName) != null)
			return true;
		return false;
	}
	
	public boolean isModifiable(String fileName) throws SQLException {
		if(findFile(fileName,"PeerFiles") != null)
			return true;
		return false;
	}
	 
	public void markDirty(String fileName) throws SQLException{
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "UPDATE PeerDownloadedFiles SET file_state= 'invalid' where file_name like '" + fileName + "'";
			statement.executeUpdate(sql);

		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	
	/**
	 * get all files in the database
	 * 
	 * @return the list
	 * @throws SQLException
	 *             the sQL exception
	 */
	public List<String> selectAllFiles() throws SQLException {

		List<String> allFiles = new ArrayList<String>();
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select file_name from PeerFiles";
			result = statement.executeQuery(sql);
			while (result.next()) {
				allFiles.add(result.getString(1));
			}
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return allFiles;
	}
	
	 
	/**
	 * get the peer info from database
	 * 
	 * @return the list
	 * @throws SQLException
	 *             the sQL exception
	 */
	public List<PeerInfo> queryAllfromPeerInfo () throws SQLException{
		List<PeerInfo> peerInfolist = new ArrayList<PeerInfo>();
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select * from PeerFiles";
			result = statement.executeQuery(sql);
			while (result.next()) {
				PeerInfo pInfo = new PeerInfo();
				pInfo.setId(result.getString(1));
				pInfo.setFilePath(result.getString(2));
				pInfo.setFileName(result.getString(3));
				pInfo.setFileSize(result.getInt(4));
				peerInfolist.add(pInfo);
			}

		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return peerInfolist;
	}
	
	
	public void addMessage(String messageId,String upstream_ip,String upstream_port, Date insert_time, Date expire_time ,String fileName) throws SQLException {
		try {
			conn = PeerHSQLDB.getConnection();
			String sql = "insert into Messages values (?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, messageId);
			stmt.setString(2, upstream_ip);
			stmt.setString(3, upstream_port);
			stmt.setTimestamp(4, new Timestamp(insert_time.getTime()));
			stmt.setTimestamp(5, new Timestamp(expire_time.getTime()));
			stmt.setString(6, fileName);
			stmt.executeUpdate();

		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public boolean checkMessage(String messageId) throws SQLException {
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select * from Messages where message_id like '" + messageId + "'";
			result = statement.executeQuery(sql);
			while (result.next()) {
				return true;
			}
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}
	
	
	public PeerMessage getPeerMessage(String messageId) throws SQLException {
		PeerMessage msg = null;
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select * from Messages where message_id like '" + messageId + "'";
			result = statement.executeQuery(sql);
			while (result.next()) {
				msg = new PeerMessage();
				msg.setMessage_id(messageId);
				msg.setUpstream_ip(result.getString("upstream_ip"));
				msg.setUpstream_port(result.getString("upstream_port"));
				msg.setTime_insert(new Date(result.getDate("time_insert").getTime()));
				msg.setTime_expire(new Date(result.getDate("time_expire").getTime()));
				msg.setFileName(result.getString("file_name"));
			}
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		return msg;
		
	}
	
	public void removeMessage(String messageId) throws SQLException{
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "delete from Messages where message_id like '" + messageId + "'";
			statement.executeUpdate(sql);

		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void removeExpiredMessages() throws SQLException{
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "delete from Messages where time_expire < sysdate";
			int i = statement.executeUpdate(sql);
			conn.commit();

		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		
	}
	
	
	public void removeAllQueryMessages() throws SQLException{
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "delete from Messages where upstream_ip like'"+InetAddress.getLocalHost().getHostAddress()+"'";
			statement.executeUpdate(sql);

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	
	
	
	
	
	
}
