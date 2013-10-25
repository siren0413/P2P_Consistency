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

import java.io.File;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cache.PeerInfo;
import com.cache.PeerMessage;
import com.client.Peer;
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

	/** The logger. */
	private final Logger LOGGER = Logger.getLogger(PeerDAO.class);
	
	/**
	 * insert into 'PeerFiles' table with file path,file name and file size
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
	public boolean insertFile(String filePath, String fileName, int fileSize, int fileVersion,
									String fileState, String ownerIP, int TTR, Date modifiedTime) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			String id = ID_Generator.generateID();
			String sql = "insert into PeerFiles values (?,?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(sql);
			stmt.setString(1, id);
			stmt.setString(2, filePath);
			stmt.setString(3, fileName);
			stmt.setInt(4, fileSize);
			stmt.setInt(5, fileVersion);
			stmt.setString(6, fileState);
			stmt.setString(7, ownerIP);
			stmt.setInt(8, TTR);
			stmt.setTimestamp(9, new Timestamp(modifiedTime.getTime()));
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
	public String findFile(String fileName) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select file_path from PeerFiles where file_name like '" + fileName + "'";
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

	public String findOwner(String fileName) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select owner_ip from PeerFiles where file_name like '" + fileName + "'";
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

	public String getFileVersion(String fileName) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select file_version from PeerFiles where file_name like '" + fileName + "'";
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
	
	public String getFileState(String fileName) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select file_state from PeerFiles where file_name like '" + fileName + "'";
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
	
	public String getOwnerIp(String fileName) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select owner_ip from PeerFiles where file_name like '" + fileName + "'";
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
	
	public int findFileTTR(String fileName) {
		int ttr = -1;
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select owner_ttr from PeerFiles where file_name like '" + fileName + "'";
			result = statement.executeQuery(sql);
			String ttrString = "-1";
			while (result.next()) {
				ttrString = result.getString(1);
			}
			ttr = Integer.parseInt(ttrString);
		} catch (SQLException e) {
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
		return ttr;
	}
	
	public Date getLastModifedTime(String fileName) {
		Date time_modifedDate = null;
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select last_modified from PeerFiles where file_name like '" + fileName + "'";
			result = statement.executeQuery(sql);
			while (result.next()) {
				time_modifedDate = new Date(result.getDate(1).getTime());
			}
			
		} catch (SQLException e) {
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
		return time_modifedDate;
	}
	
	
	/**
	 * Only could be called in Peer.modifyFile() function,
	 * with check file ownership first.
	 * 
	 * @param filePath
	 * @return true means version updated
	 * @throws SQLException
	 */
	public boolean updateFileVersion(File filePath) throws SQLException {

		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select file_version from PeerFiles where file_name like '" + filePath.getName() + "'";
			result = statement.executeQuery(sql);
			String num = "";
			while (result.next()) {
				num = result.getString(1);
			}
			LOGGER.info("File version is :" + String.valueOf(num));
			Integer versionNumber = Integer.parseInt(num);
			versionNumber++;
			sql = "UPDATE PeerFiles SET file_version='" + versionNumber + "' where file_name like '" + filePath.getName() + "'";
			statement.executeUpdate(sql);
			
			// test version update
			sql = "select file_version from PeerFiles where file_name like '" + filePath.getName() + "'";
			result = statement.executeQuery(sql);
			num = "";
			while (result.next()) {
				num = result.getString(1);
			}
			LOGGER.info("File version updated to :" + String.valueOf(num));
			
			//
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
	
	
	public void markDirty(String fileName) throws SQLException{
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "UPDATE PeerFiles SET file_state = 'invalid' where file_name like '" + fileName + "'";
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
	 *  check whether a file is in the database and its state is valid
	 * 
	 * @param fileName
	 *            the file name
	 * @return true, if successful
	 * @throws SQLException
	 *             the sQL exception
	 */
	public boolean checkFileAvailable(String fileName) throws SQLException {

		if (findFile(fileName) != null && getFileState(fileName).equals("valid"))
			return true;
		return false;
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
				pInfo.setFileVersion(result.getInt(5));
				pInfo.setFileState(result.getString(6));
				pInfo.setOwnerIp(result.getString(7));
				pInfo.setOwnerTTR(result.getInt(8));
				pInfo.setLastModifieDate(result.getDate(9));
				
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
	
	
	public Map<Object,Object> getPeerInfo(String fileName) throws SQLException{
		Map<Object,Object> map = null;
		try {
			conn = PeerHSQLDB.getConnection();
			statement = conn.createStatement();
			String sql = "select * from PeerFiles where file_name like '" + fileName + "'";
			result = statement.executeQuery(sql);
			while (result.next()) {
				map = new HashMap<Object,Object>();
				map.put("id", result.getString("id"));
				map.put("file_path", result.getString("file_path"));
				map.put("file_name", result.getString("file_name"));
				map.put("file_size", result.getInt("file_size"));
				map.put("file_version", result.getInt("file_version"));
				map.put("file_state", result.getString("file_state"));
				map.put("owner_ip", result.getString("owner_ip"));
				map.put("owner_ttr", result.getInt("owner_ttr"));
				map.put("last_modified", result.getDate("last_modified"));
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
		return map;
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
