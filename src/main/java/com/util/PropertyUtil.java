/**
 * NAME: 
 * 		PropertyUtil.java
 * 
 * PURPOSE: 
 * 		The property utility class for peer file transfer.
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 */

package com.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

public class PropertyUtil {

	/** The prop. */
	private Properties prop;

	/**
	 * Instantiates a new property util.
	 * 
	 * @param file
	 *            the file
	 * @throws FileNotFoundException
	 *             the file not found exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public PropertyUtil(String file) throws FileNotFoundException, IOException {
		prop = new Properties();
		String proFilePath = System.getProperty("user.dir") + File.separator + file;
		InputStream in = new BufferedInputStream(new FileInputStream(proFilePath));
		prop.load(in);
	}

	/**
	 * Gets the property.
	 * 
	 * @param key
	 *            the key
	 * @return the property
	 */
	public String getProperty(String key) {
		return prop.getProperty(key);

	}

	public Collection<Object> getProperties() {
		return prop.values();
	}

}
