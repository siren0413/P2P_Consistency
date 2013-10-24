package com.client;

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

	public void operations() {
		Scanner scanner = new Scanner(System.in);

		int input;
		boolean flag = true;

		while (flag) {
			System.out.println("Operation menu for peer:");
			System.out.println("Enter 1 for upload file.");
			System.out.println("Enter 2 for download file");
			System.out.println("Enter 3 for modify file");
//			System.out.println("Enter 100 number to exit");

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
					System.out.println("You choice to download file. Please give file name");
					String fileName = scanner.next();
					System.out.println("File name is : " + fileName);
					System.out.println("Please give save file path.(including file name)");
					String savePath = scanner.next();
					System.out.println("File save path is : " + savePath);
					System.out.println("Please enter the band with:");
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
				case 100:
					System.out.println("You choice to exit. Bye......");
					flag = false;
					break;
				default :
					break;
			}
		}

		System.out.println("Operation finished! Thank you!");
		scanner.close();
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

}
