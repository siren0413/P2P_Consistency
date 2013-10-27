package com;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class System_Context {

	public static int SERVICE_PORT = 1099;
	public static int BAND_WIDTH = 1;
	public static BlockingQueue<String> DOWNLOADING_QUEUE = new ArrayBlockingQueue<String>(100);
	public static int TTR = 5;
	public static boolean PUSH_APPROACH = false;
	public static boolean PULL_APPROACH = false;
	public static int TIME_TO_PULL = 5;
	
}
