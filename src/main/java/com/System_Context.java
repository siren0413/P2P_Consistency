package com;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class System_Context {

	public static int SERVICE_PORT = 2055;
	public static int BAND_WIDTH = 1;
	public static BlockingQueue<String> downloadingQueue = new ArrayBlockingQueue<String>(100);
	
	
	
}
