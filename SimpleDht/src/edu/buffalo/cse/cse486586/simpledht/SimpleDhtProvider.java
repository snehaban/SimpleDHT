package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.buffalo.cse.cse486586.simpledht.Message.MessageType;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

	static final String TAG = SimpleDhtProvider.class.getSimpleName();
	String myPort;

	SQLiteDatabase myDatabase;
	DhtHelper sqlHelper;
	Context context;
	static final String DB_NAME = "simpleDhtDB";
	static final int DB_VERSION = 1;

	static final String TABLE_NAME = "dhtTable";
	static final String KEY_FIELD = "key";
	static final String VALUE_FIELD = "value";
	static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " ( "
			+ KEY_FIELD + " TEXT UNIQUE NOT NULL, " + VALUE_FIELD + " TEXT NOT NULL );";

	public static boolean isFirst = true;
	public static boolean isWaiting = false;
	public static String queryingPort = null;
	public static HashMap<String, String> globalList = null; // for query(*)

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.v("delete", selection);
		int rows = 0;

		if (selection.equals("@")) {
			rows = myDatabase.delete(TABLE_NAME, null, null);
			
		} else if (selection.equals("*")) {
			rows = myDatabase.delete(TABLE_NAME, null, null);
			
			if (myPort.equals(ServerTask.successorPort)) 
				return rows;

			if (isFirst) // requesting port
			{
				queryingPort = myPort;
			}
			Message m = new Message();
			m.setMsgType(MessageType.DELETE_GLOBAL);
			m.setForwardingPort(ServerTask.successorPort);
			m.setQueryParam(selection);
			m.setRequestingPort(queryingPort);
			sendRequestToServer(m); // forward to server
			
			queryingPort = null; // reset
			isFirst = true;
			
		} else // simple delete
		{
			Log.v("deleted", selection);
			
			String myHashCode = ServerTask.myHashCode;
			String predecessorHashCode = ServerTask.predecessorHashCode;
			String keyHashCode;
			try {
				keyHashCode = Utility.genHash(selection);

				if (myHashCode.equals(predecessorHashCode)
						|| (keyHashCode.compareTo(myHashCode) < 0 && predecessorHashCode
								.compareTo(keyHashCode) < 0)
						|| (myHashCode.compareTo(predecessorHashCode) < 0 && (keyHashCode
								.compareTo(myHashCode) < 0 || keyHashCode
								.compareTo(predecessorHashCode) > 0))) {
					
					rows = myDatabase.delete(TABLE_NAME, KEY_FIELD + "= ?", new String[] { selection });
					return rows;
					
				} else {
					Log.v("delete fwd", selection);
					
					if (isFirst) // requesting port
					{
						queryingPort = myPort;
						isWaiting = true;
					}
					
					if (!myPort.equals(ServerTask.successorPort)) {
						Message m = new Message();
						m.setMsgType(MessageType.DELETE);
						m.setForwardingPort(ServerTask.successorPort);
						m.setRequestingPort(queryingPort);
						m.setQueryParam(selection);						
						sendRequestToServer(m); // forward to server						
					}
					
					while (isWaiting) {
						//Thread.sleep(10);
						//Log.d("DELETE", selection + " - " + myPort + "waiting for delete ack");
					}
					queryingPort = null; // reset
					isWaiting = false;
					isFirst = true;
					return -1;
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return rows;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Log.v("insert", values.toString());

		String myHashCode = ServerTask.myHashCode;
		String predecessorHashCode = ServerTask.predecessorHashCode;
		String keyHashCode;
		try {
			keyHashCode = Utility.genHash((String) values.get(KEY_FIELD));

			if (myHashCode.equals(predecessorHashCode)
					|| (keyHashCode.compareTo(myHashCode) < 0 && predecessorHashCode
							.compareTo(keyHashCode) < 0)
					|| (myHashCode.compareTo(predecessorHashCode) < 0 && (keyHashCode
							.compareTo(myHashCode) < 0 || keyHashCode
							.compareTo(predecessorHashCode) > 0)))
			{
				Log.v("inserted local", values.toString());
				long rowId = myDatabase.insertWithOnConflict(TABLE_NAME, null,
						values, SQLiteDatabase.CONFLICT_REPLACE);
				if (rowId > 0) {
					uri = ContentUris.withAppendedId(uri, rowId);
					return uri;
				}
			} else {
				Log.v("insert fwd", values.toString());
				
				if (isFirst) // requesting port
				{
					queryingPort = myPort;
					isWaiting = true;
				}
				
				Message m = new Message();
				m.setMsgType(MessageType.INSERT);
				m.setForwardingPort(ServerTask.successorPort);
				m.setRequestingPort(queryingPort);
				m.setKey((String) values.get(KEY_FIELD));
				m.setValue((String) values.get(VALUE_FIELD));				
				sendRequestToServer(m); // forward to server
				
				while (isWaiting) {
					//Log.d("INSERT", (String) values.get(KEY_FIELD) + " - " + myPort + "waiting for insert ack");
				}
				queryingPort = null; // reset
				isWaiting = false;
				isFirst = true;
				return null;
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		context = getContext();

		// Calculate the port number that this AVD listens on
		TelephonyManager tel = (TelephonyManager) context
				.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		// keep port open for listening to requests
		try {
			ServerSocket serverSocket = new ServerSocket(Integer.parseInt(Utility.SERVER_PORT));
			Log.e(TAG, "Number MY PORT - " + myPort);
			new ServerTask(context, myPort).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if (!myPort.equals(Utility.COORDINATOR_PORT)) {
			try { 
				Message m = new Message();
				m.setMsgType(MessageType.JOIN);
				m.setForwardingPort(Utility.COORDINATOR_PORT);
				m.setRequestingPort(myPort);
				new ClientTask(m).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
				
			} catch (Exception e) {
				Log.e(TAG, "Coordinator port not found");
			}
		}

		sqlHelper = new DhtHelper(context);
		myDatabase = sqlHelper.getWritableDatabase();
		if (myDatabase == null)
			return false;
		else
			return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		Cursor cursor;
		String myHashCode = ServerTask.myHashCode;
		String predecessorHashCode = ServerTask.predecessorHashCode;
		String keyHashCode;

		if (selection.equals("@")) // retrieve local entries
		{
			cursor = myDatabase.rawQuery("SELECT * from " + TABLE_NAME,
					selectionArgs);
			cursor.setNotificationUri(context.getContentResolver(), uri);
			Log.v("queryLOCAL", selection);
			return cursor;
			
		} else if (selection.equals("*")) // retrieve global entries
		{
			cursor = myDatabase.rawQuery("SELECT * from " + TABLE_NAME,
					selectionArgs);
			Log.v("queryGLOBAL",
					selection + " - Row count: " + cursor.getCount());

			// return if only one node
			if (myPort.equals(ServerTask.successorPort)) 
				return cursor;

			if (isFirst) // requesting port
			{
				queryingPort = myPort;
				isWaiting = true;
			}
			Log.v("Global Query on " + myPort, "isFirst: " + isFirst
					+ " queryingPort: " + queryingPort + " isWaiting: " + isWaiting);

			if (globalList == null)
				globalList = new HashMap<String, String>();

			try {
				int keyIndex = cursor.getColumnIndex(KEY_FIELD);
				int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
				if (keyIndex == -1 || valueIndex == -1) {
					Log.e(TAG, "Wrong columns");
					cursor.close();
					throw new Exception();
				}
				cursor.moveToFirst();
				do {
					globalList.put(cursor.getString(keyIndex),
							cursor.getString(valueIndex));
				} while (cursor.moveToNext());
				cursor.close();

			} catch (Exception e) {
				e.printStackTrace();
			}

			// forward request to successor
			Message m = new Message();
			m.setMsgType(MessageType.QUERY_GLOBAL);
			m.setForwardingPort(ServerTask.successorPort); 
			m.setRequestingPort(queryingPort);
			m.setAllEntries(globalList);
			m.setQueryParam(selection);
			Log.d("QUERY GLOBAL on " + myPort, selection + " - "
					+ "forwarding all entries to successor - " + ServerTask.successorPort);			
			sendRequestToServer(m);
			
			ServerTask.matrixCursor = null;
			while (isWaiting) {
				// Log.d("QUERY GloBAL", selection + " - " + myPort + "waiting for all entries");
			}
			queryingPort = null; // reset
			isWaiting = false;
			isFirst = true;

			if (ServerTask.matrixCursor != null) // only for LOG
				Log.d("QUERY GLOBAL", selection + " - " + myPort
						+ "Final response received. NOT WAITING any more");

			return ServerTask.matrixCursor;
		} else // simple query
		{
			try {
				keyHashCode = Utility.genHash(selection);

				if (myHashCode.equals(predecessorHashCode)
						|| (keyHashCode.compareTo(myHashCode) < 0 && predecessorHashCode
								.compareTo(keyHashCode) < 0)
						|| (myHashCode.compareTo(predecessorHashCode) < 0 && (keyHashCode
								.compareTo(myHashCode) < 0 || keyHashCode
								.compareTo(predecessorHashCode) > 0))) {
					cursor = myDatabase.rawQuery("SELECT * from " + TABLE_NAME
							+ " WHERE " + KEY_FIELD + " = ?", new String[] { selection });
					cursor.setNotificationUri(context.getContentResolver(), uri);
					Log.v("query", selection);
					return cursor;
					
				} else // forward request to successor
				{
					if (isFirst) // requesting port
					{
						queryingPort = myPort;
						isWaiting = true;
					}
					
					// forward request to successor
					Message m = new Message();
					m.setMsgType(MessageType.QUERY);
					m.setForwardingPort(ServerTask.successorPort); 
					m.setRequestingPort(queryingPort);
					m.setQueryParam(selection);
					Log.d("QUERY", selection + " - Key not found at "+ myPort 
							+ ", sending msg to successor - "+ServerTask.successorPort);					
					sendRequestToServer(m);
					
					while (isWaiting) {						
						//Log.d("QUERY", selection + " - " + myPort + " waiting...");
					}
					queryingPort = null; // reset
					isWaiting = false;
					isFirst = true;
					Log.d("QUERY", selection + " - " + myPort + "Response received.");

					return ServerTask.matrixCursor;
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private void sendRequestToServer(Message message) {
		Socket socket = null;
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), 
					Integer.parseInt(message.getForwardingPort()));

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
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	public class DhtHelper extends SQLiteOpenHelper {

		public DhtHelper(Context context) {
			super(context, DB_NAME, null, DB_VERSION);
		}

		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(CREATE_TABLE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int oldVer, int newVer) {
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
			onCreate(db);
		}
	}
}
