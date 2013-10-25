
/**
 * NAME: 
 * 		PeerInfo.java
 * 
 * PURPOSE: 
 * 		java bean which populate data from database.
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 * 
 * ALGORITHM DESCRIPTION: 
 * 		populate data from database, each attribute corresponding to the columns in database.
 * 
 */

package com.cache;

import java.util.Date;

public class PeerInfo {
	
	/** The bean class for peer information. */

	private String id;
	
	/** The file name. */
	private String fileName;
	
	/** The file path. */
	private String filePath;
	
	/** The file size. */
	private int fileSize;
	
	private int fileVersion;
	
	private String fileState;
	
	private String ownerIp;
	
	private int ownerTTR;
	
	private Date lastModifieDate;
	
	/**
	 * getter for peer id.
	 * 
	 * @return String
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * setter for peer id.
	 * 
	 * @param id
	 *            the new id
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * getter for file name.
	 * 
	 * @return String
	 */
	public String getFileName() {
		return fileName;
	}
	
	/**
	 * setter for file name.
	 * 
	 * @param fileName
	 *            the new file name
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	/**
	 * getter for file path.
	 * 
	 * @return String
	 */
	public String getFilePath() {
		return filePath;
	}
	
	/**
	 * Sets the file path.
	 * 
	 * @param filePath
	 *            the new file path
	 */
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	/**
	 * Gets the file size.
	 * 
	 * @return the file size
	 */
	public int getFileSize() {
		return fileSize;
	}
	
	/**
	 * Sets the file size.
	 * 
	 * @param fileSize
	 *            the file size
	 * @return the int
	 */
	public int setFileSize(int fileSize) {
		return this.fileSize = fileSize;
	}

	public int getFileVersion() {
		return fileVersion;
	}

	public void setFileVersion(int fileVersion) {
		this.fileVersion = fileVersion;
	}

	public String getFileState() {
		return fileState;
	}

	public void setFileState(String fileState) {
		this.fileState = fileState;
	}

	public String getOwnerIp() {
		return ownerIp;
	}

	public void setOwnerIp(String ownerIp) {
		this.ownerIp = ownerIp;
	}

	public int getOwnerTTR() {
		return ownerTTR;
	}

	public void setOwnerTTR(int ownerTTR) {
		this.ownerTTR = ownerTTR;
	}

	public Date getLastModifieDate() {
		return lastModifieDate;
	}

	public void setLastModifieDate(Date lastModifieDate) {
		this.lastModifieDate = lastModifieDate;
	}

}
