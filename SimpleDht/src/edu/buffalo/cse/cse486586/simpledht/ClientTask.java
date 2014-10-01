package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import android.os.AsyncTask;
import android.util.Log;

public class ClientTask extends AsyncTask<String, Void, Void> {

	static final String TAG = ClientTask.class.getSimpleName();
	Message message = null;

	public ClientTask(Message _message) {
		this.message = _message;
	}

	protected Void doInBackground(String... msgs) {

		Log.d(TAG, "ClientTask dIB RequestingPort - " + message.getRequestingPort());

		Socket socket = null;
		try {

			socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2,
					2 }), Integer.parseInt(message.getForwardingPort()));

			ObjectOutputStream outputStream = new ObjectOutputStream(
					new BufferedOutputStream(socket.getOutputStream()));
			outputStream.writeObject(message);
			outputStream.flush();
			outputStream.close();
			socket.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Log.d(TAG, "ClientTask - Msg sent to server");
		return null;
	}
}