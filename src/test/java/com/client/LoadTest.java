package com.client;

import java.rmi.Naming;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import com.rmi.api.IPeerTransfer;

public class LoadTest {

	private String serverIP = "192.168.1.18";
	private String serverPort = "1099";

	public LoadTest() {

	}

	@SuppressWarnings({})
	// @Test
	public void responseTimeTest() throws InterruptedException {
		Thread.sleep(10000);
		long start = System.currentTimeMillis();

		for (int i = 0; i < 10000; i++) {
			//
		}

		long end = System.currentTimeMillis();
		long responseTime = end - start;
		System.out.println(responseTime);
	}

	//@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void multiPeerResponseTimeTest() throws InterruptedException {
		Thread.sleep(10000);
		long start = System.currentTimeMillis();

		ExecutorService executor = Executors.newFixedThreadPool(500);
		CompletionService completion = new ExecutorCompletionService(executor);
		final int counter = 400;
		for (int i = 0; i < counter; i++) {
			completion.submit(new Callable() {
				public Object call() throws Exception {
					for (int i = 0; i < 10000; i++) {
						Naming.lookup("rmi://" + serverIP + ":" + serverPort + "/serverTransfer");
					}
					return null;
				}
			});
		}

		for (int i = 0; i < counter; i++) {
			completion.take(); // will block until the next sub task has
								// completed.
		}
		executor.shutdown();
		long end = System.currentTimeMillis();
		long responseTime = end - start;
		System.out.println(responseTime);

	}

	// @Test
	public void multiThreadDownloadTest() {
		long start = System.currentTimeMillis();
		Thread thread;

		for (int i = 0; i < 1000; i++) {
			thread = new Thread(new Runnable() {
				public void run() {
					downloadFromPeer();
				}
			});
			thread.start();
		}

		long end = System.currentTimeMillis();
		long responseTime = end - start;
		System.out.println(responseTime);
	}

	private void downloadFromPeer() {
		try {
			IPeerTransfer peerTransfer = (IPeerTransfer) Naming.lookup("rmi://" + serverIP + ":" + serverPort + "/peerTransfer");
			String fileName = "test.txt";
			int length = peerTransfer.getFileLength(fileName);
			byte[] byteArray = peerTransfer.obtain(fileName, 0, length);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	

}
