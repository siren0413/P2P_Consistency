/**
 * NAME: 
 * 		ClientWindow.java
 * 
 * PURPOSE: 
 * 		Open a GUI window for interacting with users
 * 
 * COMPUTER HARDWARE AND/OR SOFTWARE LIMITATIONS: 
 * 		JRE(1.7) required.
 * 
 * PROJECT: 
 * 		P2P File sharing system
 * 
 * ALGORITHM DESCRIPTION: 
 * 		initialize java swing frame and all the components in the frame.
 * 		implement the event handler of some action such as button clicked.
 * 
 */

package com.client;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JMenu;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JPanel;

import java.awt.GridLayout;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.awt.Insets;

import javax.swing.JButton;
import javax.swing.JOptionPane;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.JFileChooser;

import com.db.PeerDB.PeerHSQLDB;
import com.rmi.api.impl.PeerTransfer;
import com.util.PropertyUtil;
import com.util.SystemUtil;

import javax.swing.JTextField;
import javax.swing.JLabel;

import org.apache.log4j.Logger;

import javax.swing.JProgressBar;
import javax.swing.text.DefaultCaret;

import java.awt.Font;

public class PeerWindow {

	/** The logger. */
	private final Logger LOGGER = Logger.getLogger(PeerWindow.class);

	/** The frame. */
	private JFrame frame;

	/** The text area. */
	private JTextArea textArea;

	/** The instance. */
	private static PeerWindow instance;

	/** The option pane. */
	private final JOptionPane optionPane = new JOptionPane();

	/** The file chooser. */
	private final JFileChooser fileChooser = new JFileChooser();

	/** The text field_download file name. */
	private JTextField textField_downloadFileName;

	/** The text field_download limit. */
	private JTextField textField_downloadLimit;

	/** The progress bar. */
	private JProgressBar progressBar;

	/** The label. */
	private JLabel label;

	/** The peer. */
	private Peer peer;

	private PeerTransfer peerTransfer;

	private static BlockingQueue<String> downloadingQueue = new ArrayBlockingQueue<String>(100);;

	/** The peer registry. */
	Registry peerRegistry;
	private JTextField textField_servicePort;

	/**
	 * Launch the application.
	 * 
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					PeerWindow window = PeerWindow.getInstance();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

	}

	/**
	 * Create the application.
	 * 
	 * @throws RemoteException
	 */
	private PeerWindow() throws RemoteException {
		initialize();
		peer = new Peer(this);
		peerTransfer = new PeerTransfer(this);
		PeerHSQLDB.initDB();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {

		// window
		frame = new JFrame();
		frame.setResizable(false);
		frame.setBounds(100, 100, 800, 600);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JMenuBar menuBar = new JMenuBar();
		frame.setJMenuBar(menuBar);

		JMenu mnAbout = new JMenu("About...");
		menuBar.add(mnAbout);

		JMenuItem mntmAbout = new JMenuItem("About...");
		mntmAbout.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				JOptionPane.showMessageDialog(frame, "Vincent");
			}
		});
		mnAbout.add(mntmAbout);
		frame.getContentPane().setLayout(new GridLayout(0, 1, 0, 0));

		JPanel panel = new JPanel();
		frame.getContentPane().add(panel);
		panel.setLayout(null);

		JScrollPane scrollPane = new JScrollPane();
		scrollPane.setAutoscrolls(true);
		scrollPane.setBounds(0, 0, 794, 178);
		panel.add(scrollPane);

		textArea = new JTextArea();
		scrollPane.setViewportView(textArea);
		textArea.setMargin(new Insets(0, 5, 0, 0));
		textArea.setWrapStyleWord(true);
		textArea.setLineWrap(true);
		textArea.setEditable(false);
		DefaultCaret caret = (DefaultCaret) textArea.getCaret();
		caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);

		final JButton btnNewButton = new JButton("Share Files");
		btnNewButton.setEnabled(false);
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				fileChooser.setMultiSelectionEnabled(true);
				fileChooser.showOpenDialog(frame);
				File[] files = fileChooser.getSelectedFiles();
				for (File file : files) {
					if (peer.shareFile(file)) {
						textArea.append(SystemUtil.getSimpleTime() + "share file [" + file.getName() + "]\n");
					} else {
						textArea.append(SystemUtil.getSimpleTime() + "Unable to register file [" + file.getName() + "]\n");
					}
				}

			}
		});
		btnNewButton.setBounds(19, 266, 122, 26);
		panel.add(btnNewButton);

		final JButton btnDownloadFiles = new JButton("Download Files");
		btnDownloadFiles.setEnabled(false);
		btnDownloadFiles.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				final String fileName = textField_downloadFileName.getText();
				if ("".equals(fileName)) {
					JOptionPane.showMessageDialog(frame, "The file name is not valid!", "ERROR", JOptionPane.ERROR_MESSAGE);
					return;
				}
				JOptionPane.showMessageDialog(frame, "Please select a directory to save the file", "INFO", JOptionPane.INFORMATION_MESSAGE);
				fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				if (fileChooser.showOpenDialog(frame) == JFileChooser.APPROVE_OPTION) {
					final String path = fileChooser.getSelectedFile().getAbsolutePath();
					LOGGER.info("received user saved file path:" + path);
					File file = new File(path + File.separator + fileName);
					if (file.exists()) {
						JOptionPane.showMessageDialog(frame, "The file already exits", "ERROR", JOptionPane.ERROR_MESSAGE);
						return;
					}
					label.setText("Saved Path: " + path);
					Thread t = new Thread(new Runnable() {
						public void run() {
							peer.downloadFile(fileName, path + File.separator + fileName);
						}
					});
					LOGGER.info("start a new thread for downloading file.");
					t.start();
					
					
					
				}

			}
		});
		btnDownloadFiles.setBounds(19, 303, 122, 26);
		panel.add(btnDownloadFiles);

		textField_downloadFileName = new JTextField();
		textField_downloadFileName.setEnabled(false);
		textField_downloadFileName.setColumns(10);
		textField_downloadFileName.setBounds(151, 302, 122, 28);
		panel.add(textField_downloadFileName);

		progressBar = new JProgressBar();
		progressBar.setStringPainted(true);
		progressBar.setDoubleBuffered(true);
		progressBar.setBounds(19, 340, 254, 20);
		panel.add(progressBar);
		progressBar.setVisible(false);

		textField_downloadLimit = new JTextField();
		textField_downloadLimit.setEnabled(false);
		textField_downloadLimit.setText("1");
		textField_downloadLimit.setColumns(10);
		textField_downloadLimit.setBounds(465, 301, 45, 28);
		panel.add(textField_downloadLimit);

		label = new JLabel("");
		label.setBounds(25, 383, 458, 46);
		panel.add(label);

		JLabel lblBandwidth = new JLabel("Bandwidth");
		lblBandwidth.setFont(new Font("Tahoma", Font.PLAIN, 12));
		lblBandwidth.setBounds(405, 301, 66, 28);
		panel.add(lblBandwidth);

		JLabel lblKbs = new JLabel("KB/S");
		lblKbs.setFont(new Font("Tahoma", Font.PLAIN, 12));
		lblKbs.setBounds(522, 301, 66, 28);
		panel.add(lblKbs);

		final JButton btnClearScreen = new JButton("Clear Screen");
		btnClearScreen.setEnabled(false);
		btnClearScreen.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				textArea.setText("");
			}
		});
		btnClearScreen.setBounds(662, 189, 122, 28);
		panel.add(btnClearScreen);

		final JButton btnConnect = new JButton("connect");
		btnConnect.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try {

					try {
						LOGGER.info("Loading network configuration file...");
						new PropertyUtil("network.properties");
					} catch (FileNotFoundException e1) {
						textArea.append(SystemUtil.getSimpleTime() + "Network configuration file not found.\n");
						LOGGER.debug("Network configuration file not found", e1);
						return;
					} catch (IOException e1) {
						textArea.append(SystemUtil.getSimpleTime() + "Unknown I/O error.\n");
						LOGGER.debug("Unknown I/O error.", e1);
						return;
					}

					// register service port
					peerRegistry = LocateRegistry.createRegistry(Integer.valueOf(textField_servicePort.getText()));
					peerRegistry.rebind("peerTransfer", peerTransfer);
					LOGGER.info("open service port " + Integer.valueOf(textField_servicePort.getText()) + ", bind object peerTransfer");

					if (peerRegistry != null) {
						LOGGER.info("Register service port [" + Integer.valueOf(textField_servicePort.getText()) + "] successfully!");
					} else {
						textArea.append(SystemUtil.getSimpleTime() + "Unable to register service port [2055]!\n");
						LOGGER.error("Unable to register service port [" + Integer.valueOf(textField_servicePort.getText()) + "]!");
						return;
					}

					btnConnect.setEnabled(false);
					textField_servicePort.setEnabled(false);
					
					// peer
					peer.setPeer_service_port(textField_servicePort.getText());

					// button enable
					btnNewButton.setEnabled(true);
					btnDownloadFiles.setEnabled(true);
					btnClearScreen.setEnabled(true);
					textField_downloadFileName.setEnabled(true);
					textField_downloadLimit.setEnabled(true);
					

					TimerTask task = new TimerTask() {

						@Override
						public void run() {
							LOGGER.debug("Cleanup message table.");
							peer.cleanupMessageTable();

						}
					};

					Timer timer = new Timer();
					timer.schedule(task, 0, 1000);

				} catch (ConnectException e1) {
					JOptionPane.showMessageDialog(frame, "unable to connect to server!\nplease make sure the address is correct", "ERROR",
							JOptionPane.ERROR_MESSAGE);
					return;
				} catch (RemoteException e1) {
					JOptionPane.showMessageDialog(frame, "unknown server error! \nplease try again later", "ERROR", JOptionPane.ERROR_MESSAGE);
					return;
				}
			}
		});
		btnConnect.setBounds(283, 190, 117, 29);
		panel.add(btnConnect);

		textField_servicePort = new JTextField();
		textField_servicePort.setText("1099");
		textField_servicePort.setColumns(10);
		textField_servicePort.setBounds(151, 190, 122, 28);
		panel.add(textField_servicePort);

		JLabel lblServicePort = new JLabel("Service Port");
		lblServicePort.setBounds(67, 194, 86, 16);
		panel.add(lblServicePort);
		
		

	}

	/**
	 * Gets the text area.
	 * 
	 * @return the text area
	 */
	public JTextArea getTextArea() {
		return textArea;
	}

	/**
	 * Gets the option pane.
	 * 
	 * @return the option pane
	 */
	public JOptionPane getOptionPane() {
		return optionPane;
	}

	/**
	 * Gets the file chooser.
	 * 
	 * @return the file chooser
	 */
	public JFileChooser getFileChooser() {
		return fileChooser;
	}

	/**
	 * Gets the single instance of ClientWindow.
	 * 
	 * @return single instance of ClientWindow
	 * @throws RemoteException
	 */
	public static PeerWindow getInstance() throws RemoteException {
		if (instance == null) {
			instance = new PeerWindow();
		}
		return instance;
	}

	/**
	 * Gets the peer registry.
	 * 
	 * @return the peer registry
	 */
	public Registry getPeerRegistry() {
		return peerRegistry;
	}

	/**
	 * Gets the progress bar.
	 * 
	 * @return the progress bar
	 */
	public JProgressBar getProgressBar() {
		return progressBar;
	}

	/**
	 * Gets the frame.
	 * 
	 * @return the frame
	 */
	public JFrame getFrame() {
		return frame;
	}

	/**
	 * Gets the label.
	 * 
	 * @return the label
	 */
	public JLabel getLabel() {
		return label;
	}

	/**
	 * Gets the text field_ download limit.
	 * 
	 * @return the text field_ download limit
	 */
	public JTextField getTextField_DownloadLimit() {
		return textField_downloadLimit;
	}
	
	

	public static BlockingQueue<String> getDownloadingQueue() {
		return downloadingQueue;
	}
	
	
	public JTextField getTextField_servicePort() {
		return textField_servicePort;
	}
}
