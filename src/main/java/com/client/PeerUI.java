package com.client;
/**
 * NAME: 
 * 		PeerUI.java
 * 
 * PURPOSE: 
 * 		The menu for client to choice service
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 * 
 * ALGORITHM DESCRIPTION: 
 * 		init programs:
 * 		1. initDB()	-- init the database
 * 		2. initResource -- check configuration file. 
 * 		3. initRMIService -- register RMI
 * 		4. initThreadSercie -- init thread to clean up expired messages.
 * 		5. initPullApproach -- init thread to pull expired files
 * 
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;

import com.System_Context;
import com.dao.PeerDAO;
import com.db.PeerDB.PeerHSQLDB;
import com.rmi.api.impl.PeerTransfer;
import com.util.PropertyUtil;
import com.util.SystemUtil;

public class PeerUI {

	private Peer peer;
	
	
	
	public PeerUI() {
		peer = new Peer();
		initDB();
		initResource();
		initRMIService();
		initThreadService();
	}

	public void operations()  {
		Scanner scanner = new Scanner(System.in);

		int input;
		boolean flag = true;
		boolean method = true;
		
		while (method) {
			System.out.println("Do you want to use Push Approach or Pull Approach ?");
			System.out.println("Enter 'push' or 'pull'");
			String approach =scanner.next();
			switch (approach) {
				case "push":
					System_Context.PUSH_APPROACH = true;
					method = false;
					break;
				case "pull":
					System_Context.PULL_APPROACH = true;
					System.out.println("Please enter the time interval to pull as an integer. (eg. 10 for 10 second)");
					int timetopull = scanner.nextInt();
					System_Context.TIME_TO_PULL = timetopull;
					method = false;
					break;
				default :
					System.out.println("No such choice :" + approach+",please choice again.");
					break;
			}
		}
		
		if(System_Context.PULL_APPROACH) {
			initPullApproach();
		}
		
		while (flag) {
			System.out.println("Operation menu for peer:");

			System.out.println("Enter 1 for upload file.");
			System.out.println("Enter 2 for download file");
			System.out.println("Enter 3 for modify file");
			if(System_Context.PULL_APPROACH) {
				System.out.println("Enter 4 to refresh a file");
			}
			System.out.println("Enter 100 number to exit");


			input = scanner.nextInt();
			switch (input) {
				case 1 :
					System.out.println("You choice to upload file. Please give the absolute file path.(including file name)");
					String filePath = scanner.next();
					System.out.println("File path is : " + filePath);
					File file = new File(filePath);
					if(!file.exists()) {
						System.out.println(SystemUtil.getSimpleTime()+"File not found! Please check the file path.");
					}
					boolean sf = peer.uploadFile(file);
					if (sf)
						System.out.println("File upload successfully!");
					else
						System.out.println("File upload failed.");
					break;
				case 2 :
					System.out.println("You choice to download file. Please give file name.(eg. w1.txt)");
					String fileName = scanner.next();
					System.out.println("File name is : " + fileName);
					System.out.println("Please give save file path.(including file name, eg: /home/user/Desktop/w1.txt)");
					String savePath = scanner.next();
					System.out.println("File save path is : " + savePath);
					System.out.println("Please enter the band width:");
					int band_width = scanner.nextInt();
					System_Context.BAND_WIDTH = band_width;
					boolean dl = peer.downloadFile(fileName, savePath);
					if (dl)
						System.out.println("File download successfully!");
					else
						System.out.println("File download failed.");
					break;
				case 3 :
					System.out.println("You choice to modify file. Please give the absolute file path.(including file name)");
					String filePath2 = scanner.next();
					System.out.println("File path is : " + filePath2);
					File file2 = new File(filePath2);
					if(!file2.exists()) {
						System.out.println(SystemUtil.getSimpleTime()+"File not found! Please check the file path.");
					}
					peer.modifyFile(file2);
					break;

				case 4 :
					System.out.println("You choice to refresh an out dated file. Please give the file name.");
					String filenameString = scanner.next();
					peer.refreshFile(filenameString);
					break;

				case 100:
					System.out.println("You choice to exit. Bye......");
					flag = false;
					break;
				default :
					break;
			}
		}
		
		System_Context.PULL_APPROACH = false;
		System_Context.PUSH_APPROACH = false;
		System.out.println("Operation finished! Thank you!");
		scanner.close();
		System.exit(0);
	}

	private void initDB() {
		PeerHSQLDB.initDB();
	}

	private void initResource() {
		try {
			System.out.println("Checking network configuration file...");
			new PropertyUtil("network.properties");
		} catch (FileNotFoundException e1) {
			System.out.println("Network configuration file not found");
			return;
		} catch (IOException e1) {
			System.out.println("Unknown I/O error.");
			return;
		}
	}

	private void initRMIService() {
		// register service port
		Registry peerRegistry = null;
		try {
			peerRegistry = LocateRegistry.createRegistry(System_Context.SERVICE_PORT);
			peerRegistry.rebind("peerTransfer", new PeerTransfer());
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		System.out.println("open service port " + System_Context.SERVICE_PORT + ", to bind object peerTransfer");

		if (peerRegistry != null) {
			System.out.println("Register service port [" + System_Context.SERVICE_PORT + "] successfully!");
		} else {
			System.out.println("Unable to register service port [" + System_Context.SERVICE_PORT + "]!");
			return;
		}

	}

	private void initThreadService() {
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				System.out.println("Cleanup experied message from table.");
				try {
					new PeerDAO().removeExpiredMessages();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		};
		Timer timer = new Timer();
		timer.schedule(task, 0, 100000);

	}
	
	private void initPullApproach() {
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				peer.pull();
			}
		};
		Timer timer = new Timer();
		timer.schedule(task, 0, System_Context.TIME_TO_PULL*1000);
		
	}
	

}
