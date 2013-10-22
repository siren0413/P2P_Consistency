/*
 * 
 */
package com.cache;

import java.util.Date;

/**
 * The Class PeerMessage.
 */
public class PeerMessage {

	/** The message_id. */
	private String message_id;
	
	/** The upstream_ip. */
	private String upstream_ip;
	
	/** The upstream_port. */
	private String upstream_port;
	
	/** The time_insert. */
	private Date time_insert;
	
	/** The time_expire. */
	private Date time_expire;
	
	/** The file name. */
	private String fileName;
	
	
	/**
	 * Instantiates a new peer message.
	 */
	public PeerMessage() {
		super();
	}
	
	
	
	
	/**
	 * Instantiates a new peer message.
	 * 
	 * @param message_id
	 *            the message_id
	 * @param upstream_ip
	 *            the upstream_ip
	 * @param upstream_port
	 *            the upstream_port
	 * @param time_insert
	 *            the time_insert
	 * @param time_expire
	 *            the time_expire
	 * @param fileName
	 *            the file name
	 */
	public PeerMessage(String message_id, String upstream_ip, String upstream_port, Date time_insert, Date time_expire, String fileName) {
		super();
		this.message_id = message_id;
		this.upstream_ip = upstream_ip;
		this.upstream_port = upstream_port;
		this.time_insert = time_insert;
		this.time_expire = time_expire;
		this.fileName = fileName;
	}




	/**
	 * Gets the message_id.
	 * 
	 * @return the message_id
	 */
	public String getMessage_id() {
		return message_id;
	}
	
	/**
	 * Sets the message_id.
	 * 
	 * @param message_id
	 *            the new message_id
	 */
	public void setMessage_id(String message_id) {
		this.message_id = message_id;
	}
	
	/**
	 * Gets the upstream_ip.
	 * 
	 * @return the upstream_ip
	 */
	public String getUpstream_ip() {
		return upstream_ip;
	}
	
	/**
	 * Sets the upstream_ip.
	 * 
	 * @param upstream_ip
	 *            the new upstream_ip
	 */
	public void setUpstream_ip(String upstream_ip) {
		this.upstream_ip = upstream_ip;
	}
	
	/**
	 * Gets the upstream_port.
	 * 
	 * @return the upstream_port
	 */
	public String getUpstream_port() {
		return upstream_port;
	}
	
	/**
	 * Sets the upstream_port.
	 * 
	 * @param upstream_port
	 *            the new upstream_port
	 */
	public void setUpstream_port(String upstream_port) {
		this.upstream_port = upstream_port;
	}
	
	/**
	 * Gets the time_insert.
	 * 
	 * @return the time_insert
	 */
	public Date getTime_insert() {
		return time_insert;
	}
	
	/**
	 * Sets the time_insert.
	 * 
	 * @param time_insert
	 *            the new time_insert
	 */
	public void setTime_insert(Date time_insert) {
		this.time_insert = time_insert;
	}
	
	/**
	 * Gets the time_expire.
	 * 
	 * @return the time_expire
	 */
	public Date getTime_expire() {
		return time_expire;
	}
	
	/**
	 * Sets the time_expire.
	 * 
	 * @param time_expire
	 *            the new time_expire
	 */
	public void setTime_expire(Date time_expire) {
		this.time_expire = time_expire;
	}

	/**
	 * Gets the file name.
	 * 
	 * @return the file name
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * Sets the file name.
	 * 
	 * @param fileName
	 *            the new file name
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	
	
	
	
}
