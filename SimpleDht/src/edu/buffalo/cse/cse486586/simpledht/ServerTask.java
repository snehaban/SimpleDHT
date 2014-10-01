package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.buffalo.cse.cse486586.simpledht.Message.MessageType;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

	static final String TAG = ServerTask.class.getSimpleName();
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private Context context;
	TextView textView;

	public static String predecessorPort;
	public static String predecessorHashCode;
	public static String successorPort;
	public static String successorHashCode;
	public static String myPort;
	public static String myHashCode;

	public static Cursor matrixCursor = null;

	public ServerTask(Context _context, String _myPort) throws NoSuchAlgorithmException {
		context = _context;
		myPort = _myPort;
		myHashCode = Utility.genHash(String.valueOf((Integer.parseInt(_myPort)) / 2));
		predecessorPort = myPort;
		predecessorHashCode = myHashCode;
		successorPort = myPort;
		successorHashCode = myHashCode;
	}

	@Override
	protected Void doInBackground(ServerSocket... sockets) {
		ServerSocket serverSocket = sockets[0];
		Message msgReceived = null;
		Socket socket = null;
		ObjectInputStream inputStream = null;
		Log.d(TAG, "Server - Inside AsyncTask doInBackground");

		try {
			while (true) {

				socket = serverSocket.accept();
				inputStream = new ObjectInputStream(socket.getInputStream());
				msgReceived = (Message) inputStream.readObject();

				MessageType msgType = msgReceived.getMsgType();
				Log.d(TAG, "Server Received MessageType: " + msgType.toString());
				Log.d(TAG, "Server Requesting Port: " + msgReceived.getRequestingPort());

				String requestingPort;
				String reqHashCode;
				Message m = null;

				switch (msgType) {
				case JOIN:
					requestingPort = msgReceived.getRequestingPort();
					reqHashCode = Utility.genHash(String.valueOf(Integer
							.parseInt(requestingPort) / 2));
					
					if (myHashCode.equals(predecessorHashCode) 
							|| (reqHashCode.compareTo(myHashCode) < 0 && predecessorHashCode.compareTo(reqHashCode) < 0)
							|| (myHashCode.compareTo(predecessorHashCode) < 0 && (reqHashCode
									.compareTo(myHashCode) < 0 || reqHashCode.compareTo(predecessorHashCode) > 0))) {
						// update successor and predecessor of new node
						m = new Message();
						m.setMsgType(MessageType.UPDATE_NEIGHBOURS);
						m.setForwardingPort(requestingPort);
						m.setSuccessorPort(myPort);
						m.setPredecessorPort(predecessorPort);
						m.setRequestingPort(requestingPort);
						sendRequestToServer(m); // send to server

						// successor of current node's predecessor
						m = new Message();
						m.setMsgType(MessageType.UPDATE_NEIGHBOURS);
						m.setPredecessorPort(null);
						m.setSuccessorPort(requestingPort);
						m.setForwardingPort(predecessorPort);
						m.setRequestingPort(requestingPort);
						sendRequestToServer(m); // send to server

						// update predecessor of current node
						predecessorPort = requestingPort;
						predecessorHashCode = reqHashCode;

						Log.d(TAG, msgReceived.getMsgType().toString()
								+ " joined: " + myPort + " - Su: " + successorPort + " - Pre: " + predecessorPort);
					} else // send to successor
					{
						Log.d(TAG, "Join forwarding to successor: "
								+ msgReceived.getRequestingPort());
						m = new Message();
						m.setMsgType(MessageType.JOIN);
						m.setForwardingPort(successorPort);
						m.setRequestingPort(requestingPort);
						new ClientTask(m).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
						Log.d(TAG, "FORWARDING - Me: "+myPort+" - Su: " + successorPort + " - Pre: "+predecessorPort);
					}
					break;

				case UPDATE_NEIGHBOURS: // update own variables
					String preNode = msgReceived.getPredecessorPort();
					if (preNode != null) {
						predecessorPort = preNode;
						predecessorHashCode = Utility.genHash(String.valueOf((Integer.parseInt(predecessorPort)) / 2));
					}
					String succNode = msgReceived.getSuccessorPort();
					if (succNode != null) {
						successorPort = succNode;
						successorHashCode = Utility.genHash(String
								.valueOf(Integer.parseInt(successorPort) / 2));
					}
					Log.e(TAG, "Updated Neighbours - Me: "+myPort+" - Su: " + successorPort + " - Pre: "+predecessorPort);
					break;

				case INSERT: // call local insert()
					SimpleDhtProvider.queryingPort = msgReceived.getRequestingPort();
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = false;
					
					ContentValues cv = new ContentValues();
					cv.put(KEY_FIELD, msgReceived.getKey());
					cv.put(VALUE_FIELD, msgReceived.getValue());
					Uri uri = context.getContentResolver().insert(Utility.getUri(), cv);
					if(uri != null)
					{
						// Send acknowledgment to requester port
						Log.v("INSERT ACK", "Sending ack to requester port " + msgReceived.getRequestingPort());
						// send reply to requester
						Message msg = new Message();
						msg.setMsgType(MessageType.INSERT_ACK);
						msg.setForwardingPort(msgReceived.getRequestingPort());	
						msg.setRequestingPort(msgReceived.getRequestingPort());	
						sendRequestToServer(msg);					
					}
					SimpleDhtProvider.queryingPort = null; // reset
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = true;
					break;
					
				case INSERT_ACK:
					SimpleDhtProvider.isWaiting = false;
					break;

				case QUERY: // call local query
					matrixCursor = null;
					SimpleDhtProvider.queryingPort = msgReceived.getRequestingPort();
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = false;

					Cursor c = context.getContentResolver().query(Utility.getUri(), null, 
							msgReceived.getQueryParam(), null, null);
					if (c != null) // send response to requesting port
					{
						Log.v("queryGLOBAL", "Inside servertask - Row count: " + c.getCount());
						// send reply to requester
						Message msg = new Message();
						msg.setMsgType(MessageType.QUERY_REPONSE);
						msg.setForwardingPort(msgReceived.getRequestingPort()); 
						msg.setRequestingPort(msgReceived.getRequestingPort());
						msg.setQueryParam(msgReceived.getQueryParam());

						try {
							int keyIndex = c.getColumnIndex(KEY_FIELD);
							int valueIndex = c.getColumnIndex(VALUE_FIELD);
							if (keyIndex == -1 || valueIndex == -1) {
								Log.e(TAG, "Query - Wrong columns");
								c.close();
								throw new Exception();
							}
							c.moveToFirst();
							msg.setKey(c.getString(keyIndex));
							msg.setValue(c.getString(valueIndex));
							c.close();

						} catch (Exception e) {
							e.printStackTrace();
						}
						sendRequestToServer(msg);
					}
					SimpleDhtProvider.queryingPort = null; // reset
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = true;
					break;

				case QUERY_REPONSE:
					// received final result, return cursor
					matrixCursor = new MatrixCursor(new String[] {
							SimpleDhtProvider.KEY_FIELD,
							SimpleDhtProvider.VALUE_FIELD });
					// assign final returned cursor to matrixCursor
					((MatrixCursor) matrixCursor).addRow(new String[] {
							msgReceived.getKey(), msgReceived.getValue() });
					SimpleDhtProvider.isWaiting = false;
					break;

				case QUERY_GLOBAL: // call global query
					matrixCursor = null;
					SimpleDhtProvider.queryingPort = msgReceived.getRequestingPort();
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = false;

					// response received on requesting port
					if (SimpleDhtProvider.queryingPort.equals(myPort)) {
						HashMap<String, String> allRows = msgReceived.getAllEntries();

						matrixCursor = new MatrixCursor(new String[] {
								SimpleDhtProvider.KEY_FIELD,
								SimpleDhtProvider.VALUE_FIELD });

						// assign final returned cursor to matrixCursor and return from SimpleDhtProvider
						for (String key : allRows.keySet()) {
							((MatrixCursor) matrixCursor).addRow(new String[] {
									key, allRows.get(key) });
						}
						SimpleDhtProvider.isWaiting = false;
					} else {
						SimpleDhtProvider.globalList = msgReceived.getAllEntries();
						context.getContentResolver().query(Utility.getUri(), null, 
								msgReceived.getQueryParam(), null, null);

						Log.d("QUERY_GLOBAL " + myPort, msgReceived.getQueryParam()
										+ " - forwarding to successor " + successorPort);
					}
					SimpleDhtProvider.queryingPort = null; // reset
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = true;
					break;

				case DELETE: // call local delete()
					SimpleDhtProvider.queryingPort = msgReceived.getRequestingPort();					
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = false;
					
					int rows = context.getContentResolver().delete(Utility.getUri(),
							msgReceived.getQueryParam(), null);
					if(rows != -1)
					{
						// Send acknowledgment to requester port
						Log.v("DELETE ACK", "Sending ack to requester port " + msgReceived.getRequestingPort());
						// send reply to requester
						Message msg = new Message();
						msg.setMsgType(MessageType.DELETE_ACK);
						msg.setForwardingPort(msgReceived.getRequestingPort());	
						msg.setRequestingPort(msgReceived.getRequestingPort());						
						sendRequestToServer(msg);					
					}
					SimpleDhtProvider.queryingPort = null; // reset
					SimpleDhtProvider.isWaiting = false;
					SimpleDhtProvider.isFirst = true;
					break;
					
				case DELETE_GLOBAL: // call global delete
					SimpleDhtProvider.queryingPort = msgReceived.getRequestingPort();
					SimpleDhtProvider.isFirst = false;

					// response received on requesting port
					if (!SimpleDhtProvider.queryingPort.equals(successorPort)) {
						
						context.getContentResolver().delete(Utility.getUri(), msgReceived.getQueryParam(), null);
					}
					SimpleDhtProvider.queryingPort = null; // reset
					SimpleDhtProvider.isFirst = true;
					break;
						
				case DELETE_ACK:
					SimpleDhtProvider.isWaiting = false;
					break;
					
				default:
					break;
				}

				inputStream.close();
				socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected void sendRequestToServer(Message msg) {
		Socket socket = null;
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), 
					Integer.parseInt(msg.getForwardingPort()));

			ObjectOutputStream outputStream = new ObjectOutputStream(
					new BufferedOutputStream(socket.getOutputStream()));
			outputStream.writeObject(msg);
			outputStream.flush();
			outputStream.close();
			socket.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}