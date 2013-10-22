/*
 * 
 */
package com.rmi.api;

import java.rmi.Remote;
import java.rmi.RemoteException;


/**
 * Remote method for files transfer between peers
 * 
 */
public interface IPeerTransfer extends Remote {

	/**
	 * Check file available.
	 * 
	 * @param fileName
	 *            the file name
	 * @return true, if successful
	 * @throws RemoteException
	 *             the remote exception
	 */
	public boolean checkFileAvailable(String fileName) throws RemoteException;

	/**
	 * Obtain a file from peer.
	 * 
	 * @param fileName
	 *            the file name
	 * @param start
	 *            the start
	 * @param length
	 *            the length
	 * @return the byte[]
	 * @throws RemoteException
	 *             the remote exception
	 */
	public byte[] obtain(String fileName, int start, int length) throws RemoteException;

	/**
	 * Gets the file length.
	 * 
	 * @param fileName
	 *            the file name
	 * @return the file length
	 * @throws RemoteException
	 *             the remote exception
	 */
	public int getFileLength(String fileName) throws RemoteException;

	public void query(String messageId, int TTL, String fileName, String service_port) throws RemoteException;

	public void hitQuery(String messageId, int TTL, String fileName, String peerIP, String peerPort) throws RemoteException;

	public void queryExpire(String messageId) throws RemoteException;

}
