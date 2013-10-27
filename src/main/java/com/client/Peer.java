/**
 * NAME: 
 * 		Peer.java
 * 
 * PURPOSE: 
 * 		implement all the Peer operations.
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 * 
 * ALGORITHM DESCRIPTION: 
 * 		implement peer operations:
 * 		1. downloadFile -- download file from other peer.
 * 		2. shareFile -- share file with other peers.
 * 		3. sendSignal -- send signal to server make sure the data is consistent.
 * 		4. sendReport -- send report to server when data is not consistent.
 * 		5. listServerFile -- list all the files that available to download.
 * 		6. updateLocalDatabase -- update local database when file delete from disk.
 * 
 */
package com.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.System_Context;
import com.cache.PeerInfo;
import com.dao.PeerDAO;
import com.rmi.api.IPeerTransfer;
import com.util.ID_Generator;
import com.util.PropertyUtil;
import com.util.SystemUtil;

/**
 * The Class Peer.
 */
public class Peer {

	/** The logger. */
	private final Logger LOGGER = Logger.getLogger(Peer.class);

	/** The server ip. */
	private String serverIP;

	/** The server port. */
	private String serverPort;

	/** The peer dao. */
	private PeerDAO peerDAO;

	/**
	 * Instantiates a new peer.
	 * 
	 * @param window
	 *            the window
	 */
	public Peer() {
		peerDAO = new PeerDAO();
	}

	/**
	 * Share file.
	 * 
	 * @param file
	 *            the file
	 * @return true, if successful
	 * @throws UnknownHostException 
	 */
	public boolean uploadFile(File file) {
		try {
			// add the file to self database
			Date time_insert = new Date(System.currentTimeMillis());
			boolean result2 = peerDAO.insertFile(file.getAbsolutePath(), file.getName(), (int)file.length(), 0 ,"valid",
					InetAddress.getLocalHost().getHostAddress()+":"+String.valueOf(System_Context.SERVICE_PORT),System_Context.TTR, time_insert);
			if (result2)
				LOGGER.info("insert file[" + file.getName() + "] to local database successfully!");
			else
				return false;

		} catch (SQLException e) {
			LOGGER.error("Unable to register file [" + file.getName() + "] due to DAO error", e);
			return false;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		return true;
	}

	private class QueryProcess implements Runnable {

		private Object obj;
		private String message_id;
		private String fileName;

		public QueryProcess(Object obj, String message_id, String fileName) {
			super();
			this.obj = obj;
			this.message_id = message_id;
			this.fileName = fileName;
		}

		public void run() {
			try {
				LOGGER.debug("invoke RMI: " + "rmi://" + obj + "/peerTransfer");
				IPeerTransfer peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + obj + "/peerTransfer");
				peerTransfer.query(message_id, 10, fileName, String.valueOf(System_Context.SERVICE_PORT));
			} catch (NotBoundException e) {
				LOGGER.error("Remote call error", e);
				return;
			} catch (MalformedURLException e) {
				LOGGER.error("Remote call error", e);
				return;
			} catch (RemoteException e) {
				LOGGER.error("Remote call error", e);
				return;
			}

		}

	}

	/**
	 * Download file.
	 * 
	 * @param fileName
	 *            the file name
	 * @param savePath
	 *            the save path
	 * @return true, if successful
	 */
	public boolean downloadFile(final String fileName, String savePath) {

		// query
		Date time_insert = new Date(System.currentTimeMillis());
		Date time_expire = new Date(time_insert.getTime() + 10 * 1000);
		final String message_id = ID_Generator.generateID();

		try {
			peerDAO.addMessage(message_id, InetAddress.getLocalHost().getHostAddress(), String.valueOf(System_Context.SERVICE_PORT), time_insert, time_expire, fileName);

			LOGGER.info("Add message to database. ip:" + InetAddress.getLocalHost().getHostAddress() + " port:" + String.valueOf(System_Context.SERVICE_PORT) + " file:" + fileName);

			PropertyUtil propertyUtil = new PropertyUtil("network.properties");
			Collection<Object> values = propertyUtil.getProperties();
			for (final Object obj : values) {
				new Thread(new QueryProcess(obj, message_id, fileName)).start();
			}
		} catch (UnknownHostException e2) {
			e2.printStackTrace();
		} catch (SQLException e2) {
			e2.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		boolean result = false;
		String messageId = null;

		while (true) {

			String element = null;
			String[] destAddr = null;
			try {
				element = System_Context.DOWNLOADING_QUEUE.take();
				destAddr = element.split(":");
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				continue;
			}
			String ip = destAddr[0];
			String port = destAddr[1];
			messageId = destAddr[2];
			String file_name = destAddr[3];

			if (!file_name.equals(fileName)) {
				LOGGER.debug("Destory previous downloading thread due to fileName not equal. expect[" + fileName + "], was[" + file_name + "]");
				try {
					System_Context.DOWNLOADING_QUEUE.put(element);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return false;
			}
			
			LOGGER.debug("invoke remote object [" + "rmi://" + ip + ":" + port + "/peerTransfer]");
			IPeerTransfer peerTransfer;
			try {
				peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + ip + ":" + port + "/peerTransfer");
			} catch (Exception e1) {
				e1.printStackTrace();
				continue;
			}

			int length = 0;
			String fileState = null, ownerIp = null;
			Date time_modified = null;
			int fileVersion = -1, ttr = -1;;
			
			Map<Object,Object> map;
			try {
				map = peerTransfer.getPeerInfo(fileName);
				length = (Integer) map.get("file_size");
				fileVersion = (Integer) map.get("file_version");
				fileState = (String) map.get("file_state");
				ownerIp = (String) map.get("owner_ip");
				ttr = (Integer) map.get("owner_ttr");
				time_modified = (Date) map.get("last_modified");
			} catch (RemoteException e1) {
				e1.printStackTrace();
				continue;
			}
			int start = 0;
			int left = length;
			LOGGER.info("file size:" + length + " bytes");

			if("".equals(savePath)) {
				savePath = fileName;
			}
			File file = new File(savePath);
			OutputStream out;
			try {
				out = new FileOutputStream(file);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				continue;
			}

			byte[] buffer;
			
			System.out.println(SystemUtil.getSimpleTime()+" Start downloading...");

			while (left > 0) {
				try {
					Thread.sleep(1000);

					buffer = peerTransfer.obtain(fileName, start, 1024 * System_Context.BAND_WIDTH);

					out.write(buffer);
					left -= buffer.length;
					start += buffer.length;
					System.out.println(SystemUtil.getSimpleTime()+"downloading complete: "+ Math.round(((double)start/length)*100) + "%");
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}

			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			result = true;

			if (result) {
				LOGGER.info("download file successfully! File: [" +fileName+"],version: [" + fileVersion + "],owner address: ["+ownerIp+"],"
						+ " TTR : [" + ttr + "], last modified time is : [" + new Timestamp(time_modified.getTime()) + "]");
				System.out.println(SystemUtil.getSimpleTime() + "Download complete!\n");
				try {
					peerDAO.removeMessage(messageId);
					time_insert = new Date(System.currentTimeMillis());
					peerDAO.insertFile(savePath, fileName, length,fileVersion, fileState, ownerIp,ttr,time_modified);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				System_Context.DOWNLOADING_QUEUE.clear();
				return true;
			}

		}

	}
	
	/**
	 * check the ownership of a file
	 * 
	 * @param filePath
	 * @return true means peer owns the file, else peer cannot modify it. 
	 */
	private boolean checkOwnership(File filePath) {
		String owner = null;
		try {
			owner = peerDAO.findOwner(filePath.getName());
			if(owner == null)
				return false;
			if (owner.equals(InetAddress.getLocalHost().getHostAddress()+":"+String.valueOf(System_Context.SERVICE_PORT)))
				return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * 
	 * @param fileName
	 */
	public void refreshFile(String fileName) {
				
		try {
			if(peerDAO.findFile(fileName)== null) {
				LOGGER.info("No file: [" + fileName + "] exits.");
				return;
			}
			
			if((peerDAO.getFileState(fileName)).equals("valid")) {
				LOGGER.info("File is up to date. No need to refresh.");
				return;
			}
			
			String savePath = peerDAO.findFile(fileName);
			System.out.println("File local address:" + savePath);
			String ownerIp = peerDAO.findOwner(fileName);
			String[] ownerAddress = ownerIp.split(":");
			String ip = ownerAddress[0];
			String port = ownerAddress[1];
			
			LOGGER.info("Start downloading file: [" + fileName + "] ............");
			LOGGER.debug("invoke remote object [" + "rmi://" + ip + ":" + port + "/peerTransfer]");
			
			IPeerTransfer peerTransfer = null;
			boolean result = false;
			try {
				peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + ip + ":" + port + "/peerTransfer");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			
			int length = 0;
			String fileState = null;
			Date time_modified = null;
			int fileVersion = -1, ttr = -1;;
			
			Map<Object,Object> map;
			try {
				map = peerTransfer.getPeerInfo(fileName);
				length = (Integer) map.get("file_size");
				fileVersion = (Integer) map.get("file_version");
				fileState = (String) map.get("file_state");				
				ttr = (Integer) map.get("owner_ttr");
				time_modified = (Date) map.get("last_modified");
			} catch (RemoteException e1) {
				e1.printStackTrace();
			}
			int start = 0;
			int left = length;
			LOGGER.info("file size:" + length + " bytes");

			if("".equals(savePath)) {
				savePath = fileName;
			}
			File file = new File(savePath);
			OutputStream out = null;
			try {
				out = new FileOutputStream(file);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}

			byte[] buffer;
			
			System.out.println(SystemUtil.getSimpleTime()+" Start downloading...");

			while (left > 0) {
				try {
					Thread.sleep(1000);

					buffer = peerTransfer.obtain(fileName, start, 1024 * System_Context.BAND_WIDTH);

					out.write(buffer);
					left -= buffer.length;
					start += buffer.length;
					System.out.println(SystemUtil.getSimpleTime()+"downloading complete: "+ Math.round(((double)start/length)*100) + "%");
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			}

			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			result = true;

			if (result) {
				LOGGER.info("download file successfully! File: [" +fileName+"],version: [" + fileVersion + "],owner address: ["+ownerIp+"]");
				System.out.println(SystemUtil.getSimpleTime() + "Download complete!\n");
				try {
					peerDAO.deleteFile(fileName);
					peerDAO.insertFile(savePath, fileName, length,fileVersion,fileState, ownerIp,ttr,time_modified);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	private class ModifyProcess implements Runnable {

		private Object obj;
		private String message_id;
		private String fileName;
		private int TTL;


		public ModifyProcess(Object obj, String message_id, String fileName, int TTL) {
			super();
			this.obj = obj;
			this.message_id = message_id;
			this.fileName = fileName;
			this.TTL = TTL;
		}

		public void run() {
			try {
				LOGGER.debug("invoke RMI: " + "rmi://" + obj + "/peerTransfer");
				IPeerTransfer peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + obj + "/peerTransfer");
				peerTransfer.invalidate(message_id,TTL, fileName, String.valueOf(System_Context.SERVICE_PORT));
			} catch (NotBoundException e) {
				LOGGER.error("Remote call error", e);
				return;
			} catch (MalformedURLException e) {
				LOGGER.error("Remote call error", e);
				return;
			} catch (RemoteException e) {
				LOGGER.error("Remote call error",e);
				return;
			}

		}

	}
	
	public void modifyFile(File filePath) {
		if (!checkOwnership(filePath)) {
			LOGGER.info("Cannot modify file, do not have the authorization");
			return;
		}
		
		Date time_insert = new Date(System.currentTimeMillis());
		Date time_expire = new Date(time_insert.getTime() + 10 * 1000);
		final String message_id = ID_Generator.generateID();
		
		try {
			peerDAO.addMessage(message_id, InetAddress.getLocalHost().getHostAddress(), String.valueOf(System_Context.SERVICE_PORT), time_insert, time_expire, filePath.getName());

			LOGGER.info("Add message to database. ip:" + InetAddress.getLocalHost().getHostAddress() + " port:" + String.valueOf(System_Context.SERVICE_PORT) + " file:" + filePath.getName());
			LOGGER.info("Start modify file " + filePath.getName() + "...");
			
			boolean updateFileVersionAndTime = peerDAO.updateFileVersionAndTimeModified(filePath);
			if (updateFileVersionAndTime == false) {
				LOGGER.error("Cannot modify file [" + filePath.getName()+"]. Please check the database.");
				return ;
			}
			LOGGER.info("File [" + filePath.getName() + "] modified successfully!");
			
			if (System_Context.PUSH_APPROACH) {
				LOGGER.info("Broadcast file modified message");
				
				PropertyUtil propertyUtil = new PropertyUtil("network.properties");
				Collection<Object> values = propertyUtil.getProperties();
				for (final Object obj : values) {
					new Thread(new ModifyProcess(obj, message_id, filePath.getName(), 10)).start();
				}
			}
		} catch (UnknownHostException e2) {
			e2.printStackTrace();
		} catch (SQLException e2) {
			e2.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void pull() {
		try {
			List<PeerInfo> expiredFile = peerDAO.queryExpiredFile();
			if (expiredFile.isEmpty()) {
				LOGGER.info("No file's TTR expired.");
				return;
			}
						
			String fileName,ownerIp,ip,port;
			List<PeerInfo> returnedFileInforList = new ArrayList<PeerInfo>();
			Iterator<PeerInfo> expiredFilesIter = expiredFile.iterator();
			while(expiredFilesIter.hasNext()) {
				PeerInfo pInfo = expiredFilesIter.next();
				fileName = pInfo.getFileName();
				ownerIp = pInfo.getOwnerIp();
				String[] ipAndport = ownerIp.split(":");
				ip = ipAndport[0];
				port = ipAndport[1];
				LOGGER.debug("invoke remote object [" + "rmi://" + ip + ":" + port + "/peerTransfer]");
				IPeerTransfer peerTransfer = null;
				try {
					peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + ip + ":" + port + "/peerTransfer");
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				Map<Object, Object> peerInforMap = peerTransfer.getPeerInfo(fileName);
				
				if (peerInforMap == null) {
					LOGGER.error("Cannot get file from address:[" + ip +" : "+ port +"] for file : ["+ fileName +"]");
				}
				
				PeerInfo remotePeerInfo = new PeerInfo();
				remotePeerInfo.setId((String) peerInforMap.get("id"));
				remotePeerInfo.setFilePath((String) peerInforMap.get("file_path"));
				remotePeerInfo.setFileName((String) peerInforMap.get("file_name"));
				remotePeerInfo.setFileSize((Integer) peerInforMap.get("file_size"));
				remotePeerInfo.setFileVersion((Integer) peerInforMap.get("file_version"));
				remotePeerInfo.setFileState((String) peerInforMap.get("file_state"));
				remotePeerInfo.setOwnerIp((String) peerInforMap.get("owner_ip"));
				remotePeerInfo.setOwnerTTR((Integer) peerInforMap.get("owner_ttr"));
				remotePeerInfo.setLastModifieDate((Date) peerInforMap.get("last_modified"));								
				if (pInfo != null)
					returnedFileInforList.add(remotePeerInfo);	
			}
			
			if(expiredFile.size() != returnedFileInforList.size()) {
				LOGGER.info("The returned file infor is not the same as requested.");
			}
			
			LOGGER.debug("The size of expired files is : " + expiredFile.size()+ ", and the size of returned file is : " + returnedFileInforList.size());
			
			Iterator<PeerInfo> returnedFIterator = returnedFileInforList.iterator();
			Iterator<PeerInfo> expiredFilesIterator = expiredFile.iterator();
			while(expiredFilesIterator.hasNext()) {
				PeerInfo expFileInfo = expiredFilesIterator.next();
				String expFileName = expFileInfo.getFileName();
				int expFileVersion = expFileInfo.getFileVersion();
				System.out.println("The expired file is : [" + expFileName+"] and the file version is :["+expFileVersion+"]");
				while(returnedFIterator.hasNext()) {
					PeerInfo retFileInfo = returnedFIterator.next();
					String retFileName = retFileInfo.getFileName();
					int retFileVersion = retFileInfo.getFileVersion();
					if(expFileName.equals(retFileName)) {
						System.out.println("The returned file is : [" + expFileName+"] and the file version is :["+retFileVersion+"]");
						if (expFileVersion < retFileVersion) {
							refreshFile(expFileName);
							break;
						}else if (expFileVersion == retFileVersion){
							 peerDAO.updateFileTTR(expFileName,retFileInfo.getOwnerTTR());
							 break;
						}   
					}
				}
				returnedFIterator = returnedFileInforList.iterator();
			}
			
			LOGGER.debug("Poll expired file(s) finished.");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Update local database.
	 */

	public void updateLocalDatabase() {
		LOGGER.info("update local database");
		try {
			List<PeerInfo> list = peerDAO.queryAllfromPeerInfo();
			for (PeerInfo info : list) {
				File file = new File(info.getFilePath());
				if (!file.exists()) {
					LOGGER.info("file not found [" + file.getAbsolutePath() + "]");
					peerDAO.deleteFile(info.getFileName());
					LOGGER.info("delete file [" + file.getName() + "] from database.");
				}
			}

		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
	}

	public void cleanupMessageTable() {

		try {
			peerDAO.removeExpiredMessages();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Gets the server_ip.
	 * 
	 * @return the server_ip
	 */
	public String getServer_ip() {
		return serverIP;
	}

	/**
	 * Sets the server_ip.
	 * 
	 * @param server_ip
	 *            the new server_ip
	 */
	public void setServer_ip(String server_ip) {
		this.serverIP = server_ip;
	}

	/**
	 * Gets the server_port.
	 * 
	 * @return the server_port
	 */
	public String getServer_port() {
		return serverPort;
	}

	/**
	 * Sets the server_port.
	 * 
	 * @param server_port
	 *            the new server_port
	 */
	public void setServer_port(String server_port) {
		this.serverPort = server_port;
	}
	
	public String testQueryFileVersion(String fileName)  {
		String v = null;
		try {
			v= peerDAO.getFileVersion(fileName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return v;
	}
	
	private class PullService implements Runnable{

		public void run() {
			pull();			
		}
	}
	
	public void usingPullApproach(boolean flag) {
		try {
			(new Thread(new PullService())).start();
			Thread.sleep(System_Context.TIME_TO_PULL*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
