/**
 * NAME: 
 * 		ID_Generator.java
 * 
 * PURPOSE: 
 * 		To generate a unique ID.
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 */

package com.util;

import java.util.UUID;

public class ID_Generator {

	/**
	 * Generate unique id.
	 * 
	 * @return the string
	 */
	public static String generateID() {
		return UUID.randomUUID().toString();
	}
}
