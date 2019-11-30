package org.bdcourse.depricated.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class NetworkSocketText implements Runnable{

	public static final int SERVER_SOCKET = 12345;
	
	public static void main(String[] args) throws IOException {

		System.out.println("Server is up and will start listening on port " + NetworkSocketText.SERVER_SOCKET);
		ServerSocket serverSocket = new ServerSocket(NetworkSocketText.SERVER_SOCKET);
	    //listen for client connections forever
		while (true) {
			Socket clientSocket = serverSocket.accept();
		    NetworkSocketText handler = new NetworkSocketText();
		    handler.setClientSocket(clientSocket);
		    //client will be responsible to shut down itself
		    (new Thread(handler)).start();
		}
		
	}

	@Override
	public void run() {

		
		 try {
			//create console reader
			BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
			//get communication endpoints
			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader( new InputStreamReader(clientSocket.getInputStream()));
		
		    System.out.println("Client connected" + clientSocket + " with address:"+clientSocket.getRemoteSocketAddress());
		    
		    
		    while (true) {
		    	System.out.println("Input a new line?");
		    	String line = console.readLine();
		    	if (line.equals("exit()")) {
		    		break;
		    	} else {
		    		out.write(line);
		    		out.write("\n");
		    		//alternatively use println 
		    		//out.println(line);
		    		out.flush();
		    	}
		    }
		    
		    System.out.println("Close client");
		    clientSocket.close();
		    
		 } catch (IOException e) {
				e.printStackTrace();
	     }
	}
	
	//client socket to be used by the tread handler
	private Socket clientSocket = null;

	public void setClientSocket (Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

}
