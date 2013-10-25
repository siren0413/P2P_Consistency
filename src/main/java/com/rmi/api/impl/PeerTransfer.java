/**
 * NAME: 
 * 		PeerTransfer.java
 * 
 * PURPOSE: 
 * 		RMI class. operations between peer and peer.
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 * 
 * ALGORITHM DESCRIPTION: 
 * 		1. obtain -- get byte array of the file from other peer.
 * 		2. getFileLength -- get the length of file which to be download
 * 		3. getFilePath -- get the file path.
 * 		4. checkFileAvailable -- check if the file is exist on the peer.
 * 
 */

package com.rmi.api.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Logger;

import com.System_Context;
import com.cache.PeerMessage;
import com.client.PeerUI;
import com.dao.PeerDAO;
import com.rmi.api.IPeerTransfer;
import com.util.PropertyUtil;
import com.util.SystemUtil;

@SuppressWarnings("serial")
public class PeerTransfer extends UnicastRemoteObject implements IPeerTransfer {

	private Logger LOGGER = Logger.getLogger(PeerTransfer.class);
	private PeerDAO peerDAO = new PeerDAO();

	
	public PeerTransfer() throws RemoteException {
		super();
	}


	// download a file from a peer
	public byte[] obtain(String fileName, int start, int length) throws RemoteException {

		// get byte[] from other peers;
		try {
			String filePath = peerDAO.findFile(fileName);
			InputStream is = new FileInputStream(filePath);
			ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
			byte[] buffer = new byte[length];
			int readSize;
			is.skip(start);
			if ((readSize = is.read(buffer, 0, length)) != -1) {
				byteArray.write(buffer, 0, readSize);
			}
			is.close();
			return byteArray.toByteArray();

		} catch (FileNotFoundException e) {
			LOGGER.error("file: " + fileName + " not found", e);
			return null;
		} catch (IOException e) {
			LOGGER.error("unable to read file", e);
			return null;
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return null;

	}

	public int getFileLength(String fileName) throws RemoteException {
		String filePath;
		File file = null;
		try {
			filePath = peerDAO.findFile(fileName);
			file = new File(filePath);
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return (int) file.length();
	}

	public String findFile(String fileName) throws RemoteException {
		String filePath = null;
		try {
			filePath = peerDAO.findFile(fileName);
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return filePath;
	}
	
	public String getFileVersion(String fileName) throws RemoteException {
		String fileVersion = null;
		try {
			fileVersion = peerDAO.getFileVersion(fileName);
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return fileVersion;
	}
	
	public String getFileState(String fileName) throws RemoteException {
		String fileState = null;
		try {
			fileState = peerDAO.getFileState(fileName);
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return fileState;
	}
	
	public String getOwnerIp(String fileName) throws RemoteException {
		String ownerIp = null;
		try {
			ownerIp = peerDAO.getOwnerIp(fileName);
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return ownerIp;
	}
	
	public String getFilePath(String fileName) throws RemoteException {
		try {
			return peerDAO.findFile(fileName);
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return null;
	}

	public boolean checkFileAvailable(String fileName) throws RemoteException {
		try {
			return peerDAO.checkFileAvailable(fileName);
		} catch (SQLException e) {
			LOGGER.error("DAO error", e);
		}
		return false;
	}

	public void query(String messageId, int TTL, String fileName, String service_port) throws RemoteException {
		String clienthost;
		try {
			clienthost = RemoteServer.getClientHost();

			InetAddress ia = java.net.InetAddress.getByName(clienthost);
			String clentIp = ia.getHostAddress();

			LOGGER.info("Received peer message. peer IP[" + clentIp + "]");

			if (TTL == 0) {
				LOGGER.debug("TTL=" + TTL + " query expire.");
				return;
			} else {
				TTL -= 1;
				if (peerDAO.checkMessage(messageId)) {
					LOGGER.debug("messageid:" + messageId + " already exist.");
					return;
				} else {
					Date time_insert = new Date(System.currentTimeMillis());
					Date time_expire = new Date(time_insert.getTime() + TTL * 1000);
					peerDAO.addMessage(messageId, clentIp, service_port, time_insert, time_expire, fileName);
					LOGGER.debug("add message to database, message from peer:" + clentIp);
				}

				// check if the ip and port and message is already in local
				// database. if yes, then ignore, if no, continue;
				// put ip and port and messageId into local database
				// query local database see if we have the file.

				if (peerDAO.checkFileAvailable(fileName)) {
					LOGGER.debug("hitquery, looping back to sender.");
					LOGGER.debug("invoke remote object [" + "rmi://" + clentIp + ":" + service_port + "/peerTransfer]");
					IPeerTransfer peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + clentIp + ":" + service_port + "/peerTransfer");
					peerTransfer.hitQuery(messageId, TTL, fileName, InetAddress.getLocalHost().getHostAddress(), String.valueOf(System_Context.SERVICE_PORT));

				}

				// forward to neighbors
				PropertyUtil propertyUtil = new PropertyUtil("network.properties");
				Collection<Object> values = propertyUtil.getProperties();
				for (Object obj : values) {
					new Thread(new QueryProcess(obj, messageId, fileName, TTL)).start();
				}

			}

		} catch (ServerNotActiveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private class QueryProcess implements Runnable {

		private Object obj;
		private String message_id;
		private String fileName;
		private int TTL;

		public QueryProcess(Object obj, String message_id, String fileName, int TTL) {
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
				peerTransfer.query(message_id, TTL, fileName,  String.valueOf(System_Context.SERVICE_PORT));
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

	public void hitQuery(String messageId, int TTL, String fileName, String peerIP, String peerPort) throws RemoteException {
		try {
			PeerMessage msg = peerDAO.getPeerMessage(messageId);
			if (msg == null) {
				LOGGER.debug("message:" + messageId + " already deleted.");
				return;
			}
			if (msg.getUpstream_ip().equals(InetAddress.getLocalHost().getHostAddress())) {
				// the original sender, and put the peerIP and peerPort and
				// fileName in queue.

				System_Context.DOWNLOADING_QUEUE.put(peerIP+":"+peerPort+":"+messageId+":"+fileName); //+":"+filePath+":"+fileVersion+":"+fileState+":"+ownerIp);
			} else {
				String upstream_ip = msg.getUpstream_ip();
				String upstream_port = msg.getUpstream_port();
				LOGGER.debug("invoke remote object [" + "rmi://" + upstream_ip + ":" + upstream_port + "/peerTransfer]");
				IPeerTransfer peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + upstream_ip + ":" + upstream_port + "/peerTransfer");
				peerTransfer.hitQuery(messageId, TTL, fileName, peerIP, peerPort);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void queryExpire(String messageId) throws RemoteException {

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
				LOGGER.error("Remote call error", e);
				return;
			}

		}

	}

	public void invalidate(String messageId,int TTL,String fileName, String service_port) throws RemoteException{
		String clienthost;
		try {
			clienthost = RemoteServer.getClientHost();

			InetAddress ia = java.net.InetAddress.getByName(clienthost);
			String clentIp = ia.getHostAddress();

			LOGGER.info("Received peer invalidate message from peer IP[" + clentIp + "]");

			if (TTL==0) {
				LOGGER.debug("TTL=" + TTL + " query expire.");
				return;
			}else {
				TTL -= 1;
				if (peerDAO.checkMessage(messageId)) {
					LOGGER.debug("messageid:" + messageId + " already exist.");
					return;
				} else {
					Date time_insert = new Date(System.currentTimeMillis());
					Date time_expire = new Date(time_insert.getTime() + 10 * 1000);
					peerDAO.addMessage(messageId, clentIp, service_port, time_insert, time_expire, fileName);
					LOGGER.debug("Add message to database, message from peer:" + clentIp);
				}
				
				// check database, if cached file exits then mark it dirty
				if (peerDAO.checkFileAvailable(fileName)) {
					LOGGER.debug("Got file in database,update file as invalid.");
					peerDAO.markDirty(fileName);
					
				}

				// forward to neighbors, broadcast the message
				PropertyUtil propertyUtil = new PropertyUtil("network.properties");
				Collection<Object> values = propertyUtil.getProperties();
				for (Object obj : values) {
					new Thread(new ModifyProcess(obj, messageId, fileName, TTL)).start();
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}


			
	}

}
