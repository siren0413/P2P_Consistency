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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

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

	/** The peer_service_port. */
	private String peer_service_port;

	/** The peer dao. */
	private PeerDAO peerDAO;

	private static BlockingQueue<String> downloadingQueue = new ArrayBlockingQueue<String>(100);
	
	public static BlockingQueue<String> getDownloadingQueue() {
		return downloadingQueue;
	}
	
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
	 */
	public boolean shareFile(String fileName, String filePath) {
		try {
			// add the file to self database
			boolean result2 = peerDAO.uploadFile(filePath, fileName, 100, 0);
			if (result2)
				LOGGER.info("insert file[" + fileName + "] to local database successfully!");
			else
				return false;

		} catch (SQLException e) {
			LOGGER.error("Unable to register file [" + fileName + "] due to DAO error", e);
			return false;
		}

		return true;
	}
	
	private class ModifyProcess implements Runnable{
		private Object obj;
		private String message_id;
		private String fileName;
		
		public ModifyProcess(Object obj,String fileName) {
			super();
			this.obj = obj;
			this.fileName = fileName;
		}

		public void run() {
			try {
				LOGGER.debug("invoke RMI: " + "rmi://" + obj + "/peerTransfer");
				IPeerTransfer peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + obj + "/peerTransfer");
				peerTransfer.invalidate(message_id,getServer_ip(), getServer_port(), fileName);
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
	
	public void modifyFile(String fileName, String filePath) {
		try {
			LOGGER.info("Start modify file " + fileName + "...");
			boolean updateFileVersion = peerDAO.updateFileVersion(fileName);
			if (updateFileVersion == false) {
				LOGGER.info("Cannot modify file [" + fileName+"]. Please check the database.");
				return ;
			}
			LOGGER.info("File [" + fileName + "] modified successfully!");
			LOGGER.info("Broadcast file modified message");
			
			PropertyUtil propertyUtil = new PropertyUtil("network.properties");
			Collection<Object> values = propertyUtil.getProperties();
			for (final Object obj : values) {
			new Thread(new ModifyProcess(obj, fileName)).start();
		}
			
		} catch (Exception e) {
			LOGGER.error("ModifyFile[" + fileName +"] failed!");
			e.printStackTrace();
		}
		
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
				peerTransfer.query(message_id, 10, fileName, peer_service_port);
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
			peerDAO.addMessage(message_id, InetAddress.getLocalHost().getHostAddress(), peer_service_port, time_insert, time_expire, fileName);

			LOGGER.info("Add message to database. ip:" + InetAddress.getLocalHost().getHostAddress() + " port:" + peer_service_port + " file:" + fileName);

			PropertyUtil propertyUtil = new PropertyUtil("network.properties");
			Collection<Object> values = propertyUtil.getProperties();
			System.out.println("number of neibhours: " + values.size());
			for (final Object obj : values) {
				System.out.println("obj is :" + obj.toString());
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
				element = downloadingQueue.take();
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
					downloadingQueue.put(element);
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
			try {
				length = peerTransfer.getFileLength(fileName);
			} catch (RemoteException e1) {
				e1.printStackTrace();
				continue;
			}
			int start = 0;
			int left = length;
			LOGGER.info("file size:" + length + " bytes");

			File file = new File(savePath+"\\"+fileName);
			OutputStream out;
			try {
				out = new FileOutputStream(file);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				continue;
			}

			byte[] buffer;
			
//			LOGGER.info("download speed:" + Integer.valueOf(window.getTextField_DownloadLimit().getText()) + " KB/S");

//			window.getTextArea().append(SystemUtil.getSimpleTime() + "Start downloading...\n");

			while (left > 0) {
				try {
			//		Thread.sleep(1000);

					buffer = peerTransfer.obtain(fileName, start, 1024 * Integer.valueOf(5));
					System.out.println("buffer :" + buffer.length);
					out.write(buffer);
					left -= buffer.length;
					start += buffer.length;
//					window.getProgressBar().setValue(start);
//					window.getProgressBar().setIndeterminate(false);
//					window.getProgressBar().repaint();
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
				LOGGER.info("download file successfully!");
//				window.getTextArea().append(SystemUtil.getSimpleTime() + "Download complete!\n");
				try {
					peerDAO.removeMessage(messageId);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				Peer.getDownloadingQueue().clear();
				return true;
			}
//			window.getProgressBar().setVisible(false);

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

	/**
	 * Gets the peer_service_port.
	 * 
	 * @return the peer_service_port
	 */
	public String getPeer_service_port() {
		return peer_service_port;
	}

	/**
	 * Sets the peer_service_port.
	 * 
	 * @param peer_service_port
	 *            the new peer_service_port
	 */
	public void setPeer_service_port(String peer_service_port) {
		this.peer_service_port = peer_service_port;
	}

}
