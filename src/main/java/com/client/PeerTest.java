package com.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import com.db.PeerDB.PeerHSQLDB;
import com.rmi.api.impl.PeerTransfer;
import com.util.PropertyUtil;

public class PeerTest {

	final Peer peer;
	PeerTransfer peerTransfer;
	Registry peerRegistry;
	
	public PeerTest() throws RemoteException {
		peer = new Peer();
		peerTransfer = new PeerTransfer();
		PeerHSQLDB.initDB();
		
		try {

			try {
				System.out.println("Loading network configuration file...");
				new PropertyUtil("network.properties");
			} catch (FileNotFoundException e1) {
				System.out.println("Network configuration file not found");
				return;
			} catch (IOException e1) {
				System.out.println("Unknown I/O error.");
				return;
			}

			// register service port
			int port = 2055;
			peerRegistry = LocateRegistry.createRegistry(port);
			peerRegistry.rebind("peerTransfer", peerTransfer);
			System.out.println("open service port "+ port + ", to bind object peerTransfer");

			if (peerRegistry != null) {
				System.out.println("Register service port [" + port + "] successfully!");
			} else {
				System.out.println("Unable to register service port [" + port + "]!");
				return;
			}
			
			// peer
			peer.setPeer_service_port(""+port);

			TimerTask task = new TimerTask() {

				@Override
				public void run() {
					System.out.println("Cleanup message table.");
					peer.cleanupMessageTable();

				}
			};

			Timer timer = new Timer();
//			timer.schedule(task, 0, 1000);
			
			}catch (RemoteException e1) {
			e1.printStackTrace();
			return;
		}
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
			System.out.println("Enter other number to exit");

			input = scanner.nextInt();
			switch (input) {
				case 1 :
					System.out.println("You choice to upload file. Please give the file path.");
					String filePath = scanner.next();
					System.out.println("File path is : " + filePath);
					System.out.println("Please give the file name");
					String fileName2 = scanner.next();
					System.out.println("The file name is: " + fileName2);
					boolean sf = peer.shareFile(fileName2, filePath);
					if(sf)
						System.out.println("File upload successfully!");
					else
						System.out.println("File upload failed.");
					break;
				case 2 :
					System.out.println("You choice to download file. Please give file name and saved path");
					String fileName = scanner.next();
					System.out.println("File name is : " + fileName);
					String savePath = scanner.next();
					System.out.println("File save path is : " + savePath);
					boolean dl = peer.downloadFile(fileName, savePath);
					if(dl)
						System.out.println("File download successfully!");
					else
						System.out.println("File download failed.");
					
				case 3 :
					System.out.println("You choice to modify file. Please give file path");
					String filePath2 = scanner.next();
					System.out.println("File path is : " + filePath2);
					System.out.println("Please give the file name");
					String filename1 = scanner.next();
					System.out.println("File name is : " + filename1);
					peer.modifyFile(filename1, filePath2);
					
				default :
					break;
			}
			System.out.println("Do you want to continue?");
			System.out.println("Enter 'no' to exit. Enter 'yes' to continue.");
			String con = scanner.next();
			if (con.equals("no"))						
				flag = false;
			
		}
		
		System.out.println("Operation finished! Thank you!");
		scanner.close();
	}
	
	public static void main(String[] args) throws RemoteException {
		PeerTest pt = new PeerTest();
		pt.operations();
		
	}
}



