package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	// Ports
	private static final int SERVER_PORT = 10000;
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static ArrayList<String> ports = new ArrayList<String>(Arrays.asList(REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4));

	private final String Key = "key";
	private final String Value = "value";
	private String myPort;
	private String mysuccessor = null;
	String failPort = null;

	Map<String, String> global_key = new HashMap<String, String>(); //storing hashed port value & the 4-digit port number
	ArrayList<String> hash_key = new ArrayList<String>(); //storing the hashed port values
	Map<String, String> dummy_map = new HashMap<String, String>();
	ArrayList<message> recovered_msgs = new ArrayList<message>();
	ArrayList<message> deleted_msgs = new ArrayList<message>();
	private final Lock lock=new ReentrantLock();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@"))
		{
			Log.i(TAG, "In local delete method");
			String[] files = getContext().fileList();
			for (String f : files)
			{
				Log.i(TAG,"Deleting : "+f);
				getContext().deleteFile(f);

			}

		}
		else if(selection.equals("*"))
		{
				Log.i(TAG, "In global delete method");
				String[] files = getContext().fileList();
				for (String f : files)
				{
					Log.i(TAG,"Deleting : "+f);
					getContext().deleteFile(f);
				}

				Log.i(TAG,"Deleted "+myPort+" entries; forwarding to other ports");

			for(String p : ports)
			{
				if(!p.equals(myPort))
				{
					Socket socket = null;
					try {
						message m = new message(null, null, String.valueOf(Integer.parseInt(myPort)/2), 3, dummy_map);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p));
						Log.i(TAG, "Sending to " + p);
						OutputStream outputStream = socket.getOutputStream();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
						objectOutputStream.writeObject(m);

					}catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}
		else
		{
			String hashed_selection=null;
			try {
				hashed_selection = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			String deletePort = null;
			int deleteIndex = -1;

			Collections.sort(hash_key);
			for(String k : hash_key)
			{
				if(hashed_selection.compareTo(k)<0)
				{
					deletePort = global_key.get(k);
					deleteIndex = hash_key.indexOf(k);
					break;
				}
			}

			if (deletePort == null)
			{
				Log.i(TAG,"Hashed key greater than all keys");
				deletePort = global_key.get(hash_key.get(0));
				deleteIndex = 0;
			}

			String successor1 = null;
			String successor2 = null;
			String successor3 = null;


			if(deleteIndex<=1)
			{
				successor1 = global_key.get(hash_key.get(deleteIndex+1));
				successor2 = global_key.get(hash_key.get(deleteIndex+2));
				successor3 = global_key.get(hash_key.get(deleteIndex+3));
			}
			else if(deleteIndex == 2)
			{
				successor1 = global_key.get(hash_key.get(3));
				successor2 = global_key.get(hash_key.get(4));
				successor3 = global_key.get(hash_key.get(0));
			}
			else if (deleteIndex == 3)
			{
				successor1 = global_key.get(hash_key.get(4));
				successor2 = global_key.get(hash_key.get(0));
				successor3 = global_key.get(hash_key.get(1));
			}
			else
			{
				successor1 = global_key.get(hash_key.get(0));
				successor2 = global_key.get(hash_key.get(1));
				successor3 = global_key.get(hash_key.get(2));
			}

			Log.i(TAG, "Deleting key in ports "+deletePort+" "+successor1+" "+successor2);

			String[] delete_ports = {deletePort, successor1, successor2};

			for (String p : delete_ports)
			{
				Log.i(TAG, "Sending message to relevant ports");
				Socket socket = null;
				try {
					message m = new message(selection, null, String.valueOf(Integer.parseInt(myPort)/2), 4, dummy_map);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p)*2);
					Log.i(TAG, "Sending to " + p);
					socket.setSoTimeout(200);

					OutputStream outputStream = socket.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(m);

					DataInputStream ds = new DataInputStream(socket.getInputStream());
					String ack = ds.readUTF();
					Log.i(TAG, ack+" received from "+p);
				}catch (SocketTimeoutException e) {
					Log.e(TAG, "Socket timeout exception caught");
				}catch (IOException e) {
					Log.e(TAG, "Socket IOException at delete; Failed port caught!");
					failPort = p;
					Log.e(TAG,"Failed Port is : "+failPort);
				}

			}

//			Log.i(TAG,failPort);

			if (failPort!=null)
			{
				try {
					if (failPort.equals(deletePort))
					{
						message m = new message(selection, null, deletePort, 8, dummy_map);
						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor1) * 2);
						Log.i(TAG, "Sending failed port messages to " + successor1);
						OutputStream outputStream = socket1.getOutputStream();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
						objectOutputStream.writeObject(m);
					}

					else if (failPort.equals(successor1))
					{
						message m = new message(selection, null, successor1, 8, dummy_map);
						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor2) * 2);
						Log.i(TAG, "Sending failed port messages to " + successor2);
						OutputStream outputStream = socket1.getOutputStream();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
						objectOutputStream.writeObject(m);
					}

					else if (failPort.equals(successor2))
					{
						message m = new message(selection, null, successor2, 8, dummy_map);
						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor3) * 2);
						Log.i(TAG, "Sending failed port messages to " + successor3);
						OutputStream outputStream = socket1.getOutputStream();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
						objectOutputStream.writeObject(m);
					}
				}catch (IOException e){
					Log.e(TAG,"Recovery messages not sent");
				}
			}

		}

		return 0;
	}

	public void deleteVal(Uri uri, String key)
	{
		Log.i(TAG, "In deleteVal method");
		getContext().deleteFile(key);
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.i(TAG, "In Insert method "+myPort);

		String filename = (String) values.get(Key);
		String val = (String) values.get(Value);
//		FileOutputStream outputStream;
		Log.i(TAG,"Inserting "+filename+" "+val);


		String hashed_key = null;
		try {
			hashed_key = genHash(filename);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		System.out.println("Hashed key : "+hashed_key);

		String insertPort = null;
		int insertIndex = -1;


		for(String k : hash_key)
		{
			Log.i(TAG,"Comparing "+hashed_key+" and "+k);
			if(hashed_key.compareTo(k)<0)
			{
				System.out.println("In insert-if statement");
				insertPort = global_key.get(k);
				insertIndex = hash_key.indexOf(k);
				break;

			}
		}

		if (insertPort == null)
		{
			Log.i(TAG,"Hashed key greater than all keys");
			insertPort = global_key.get(hash_key.get(0));
			insertIndex = 0;
		}

		String successor1 = null;
		String successor2 = null;
		String successor3 = null;


		if(insertIndex<=1)
		{
			successor1 = global_key.get(hash_key.get(insertIndex+1));
			successor2 = global_key.get(hash_key.get(insertIndex+2));
			successor3 = global_key.get(hash_key.get(insertIndex+3));
		}
		else if (insertIndex==2)
		{
			successor1 = global_key.get(hash_key.get(3));
			successor2 = global_key.get(hash_key.get(4));
			successor3 = global_key.get(hash_key.get(0));

		}
		else if (insertIndex==3)
		{
			successor1 = global_key.get(hash_key.get(4));
			successor2 = global_key.get(hash_key.get(0));
			successor3 = global_key.get(hash_key.get(1));
		}
		else
		{
			successor1 = global_key.get(hash_key.get(0));
			successor2 = global_key.get(hash_key.get(1));
			successor3 = global_key.get(hash_key.get(2));
		}

		Log.i(TAG, "Inserting key in ports "+insertPort+" "+successor1+" "+successor2);


		String[] insert_ports = {insertPort, successor1, successor2};

		for (String p : insert_ports)
		{
			Log.i(TAG, "Sending message to relevant ports");
			Socket socket = null;
			try {
				message m = new message(filename, val, insertPort, 0, dummy_map);
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p)*2);
				Log.i(TAG, "Sending to " + p);
				socket.setSoTimeout(200);

				OutputStream outputStream = socket.getOutputStream();
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
				objectOutputStream.writeObject(m);

				DataInputStream ds = new DataInputStream(socket.getInputStream());
				String ack = ds.readUTF();
				Log.i(TAG, ack+" received from "+p);
			}catch (SocketTimeoutException e) {
				Log.e(TAG, "Socket timeout exception caught");
			}catch (IOException e) {
				Log.e(TAG, "Socket IOException at insert; Failed port caught!");
			failPort = p;
			Log.e(TAG,"Failed Port is : "+failPort);
			}

		}

//		Log.i(TAG,failPort);

		if(failPort!=null)
		{
			try {
				if (failPort.equals(insertPort))
				{
					message m = new message(filename, val, insertPort, 5, dummy_map);
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor1) * 2);
					Log.i(TAG, "Sending failed port messages to " + successor1);
					OutputStream outputStream = socket1.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(m);
				}

				else if (failPort.equals(successor1))
				{
					message m = new message(filename, val, successor1, 5, dummy_map);
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor2) * 2);
					Log.i(TAG, "Sending failed port messages to " + successor2);
					OutputStream outputStream = socket1.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(m);
				}

				else if (failPort.equals(successor2))
				{
					message m = new message(filename, val, successor2, 5, dummy_map);
					Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor3) * 2);
					Log.i(TAG, "Sending failed port messages to " + successor3);
					OutputStream outputStream = socket1.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(m);
				}
			}catch (IOException e){
				Log.e(TAG,"Recovery messages not sent");
			}
		}

		return null;
	}

	public void insertVal(Uri uri, String key, String value)
	{
		FileOutputStream outputStream1;
		try {
			Log.i(TAG,"Writing "+key+" in port "+myPort);
			outputStream1 = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			outputStream1.write(value.getBytes());
			outputStream1.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, message, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket1 = sockets[0];
			while (true) {
				try {
					Socket socket1 = serverSocket1.accept();
					ObjectInputStream in = new ObjectInputStream(socket1.getInputStream());
					message msg1 = (message) in.readObject();
					Log.i(TAG, "Connection Accepted by port " + myPort + " from " + msg1.originalport);
					System.out.println(msg1.op_type);

					if(msg1.op_type == 0) //insert-request
					{
						DataOutputStream d = new DataOutputStream(socket1.getOutputStream());
						d.writeUTF("ACK");
						String key = msg1.key;
						String val = msg1.value;
						Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						insertVal(uri, key, val);
					}

					else if(msg1.op_type == 1) //global-query-request
					{
						Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						Cursor matrixCursor1 = query(uri,null,"@",null,null);
						matrixCursor1.moveToPosition(-1);
						while(matrixCursor1.moveToNext())
						{
							msg1.key_val.put(matrixCursor1.getString(matrixCursor1.getColumnIndex(Key)),matrixCursor1.getString(matrixCursor1.getColumnIndex(Value)));
						}
						message m2 = new message(msg1.key, msg1.value, String.valueOf(Integer.parseInt(myPort)/2), 1, msg1.key_val);
						OutputStream outputStream = socket1.getOutputStream();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
						objectOutputStream.writeObject(m2);
					}

					else if(msg1.op_type == 2) //select-query-request
					{
						String key = msg1.key;
						Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						String value = queryVal(uri, key);
						Log.i(TAG,"Value retieved is : "+value);
						msg1.key_val.put(key, value);
						message m3 = new message(msg1.key, msg1.value, String.valueOf(Integer.parseInt(myPort)/2), 2, msg1.key_val);
						OutputStream outputStream = socket1.getOutputStream();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
						objectOutputStream.writeObject(m3);
					}

					else if(msg1.op_type == 3) //global-delete-request
					{
						Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						delete(uri, "@",null);
					}

					else if(msg1.op_type == 4) //select-delete-request
					{
						DataOutputStream d = new DataOutputStream(socket1.getOutputStream());
						d.writeUTF("ACK");
						String key = msg1.key;
						Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						deleteVal(uri, key);
					}

					else if(msg1.op_type == 5) //storing recovery messages for failed port
					{
						Log.i(TAG,"Received recovery messages for port "+msg1.originalport);
						recovered_msgs.add(msg1);
					}

					else if(msg1.op_type == 6) //sending recovery and delete msgs to recovered port
					{
						if(!recovered_msgs.isEmpty())
						{
							for (message m : recovered_msgs)
							{
								message rmsg = new message(m.key, m.value, m.originalport, 7, m.key_val);
								publishProgress(rmsg);
							}
						}
						if(!deleted_msgs.isEmpty())
						{
							for (message m : deleted_msgs)
							{
								message dmsg = new message(m.key, m.value, m.originalport, 9, m.key_val);
								publishProgress(dmsg);
							}
						}

						recovered_msgs.clear();
						deleted_msgs.clear();
					}

					else if(msg1.op_type == 7) //inserting recovered msgs
					{
						Log.i(TAG,"Recieved lost messages");
						String key = msg1.key;
						String val = msg1.value;
						Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						insertVal(uri, key, val);
					}

					else if(msg1.op_type == 8) //storing msgs to be deleted for failed port
					{
						Log.i(TAG,"Received delete messages for port "+msg1.originalport);
						deleted_msgs.add(msg1);
					}

					else if(msg1.op_type == 9) //deleting recovered msg requests
					{
						String key = msg1.key;
						Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
						deleteVal(uri, key);
					}

				}catch (OptionalDataException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}

		protected void onProgressUpdate(message... msgs)
		{
			message m = msgs[0];

			if(m.op_type == 7)
			{
				Log.i(TAG,"Sending recovered insert requests");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
			}

			else if(m.op_type == 9)
			{
				Log.i(TAG,"Sending recovered delete requests");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, m);
			}


		}


	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		String myporthash = null;

		try {
			myporthash = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		for(String p : ports)
		{
			String port = String.valueOf(Integer.parseInt(p)/2);
			String hash = null;
			try {
				hash = genHash(port);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			hash_key.add(hash);
			global_key.put(hash,port);

		}

		Collections.sort(hash_key);

		for(int i=0; i<5; i++)
		{
			System.out.println(hash_key.get(i));
			System.out.println(global_key.get(hash_key.get(i)));
		}


		int myindex = hash_key.indexOf(myporthash);
		System.out.println(myindex);

		if(myindex<=3)
		{
			mysuccessor = global_key.get(hash_key.get(myindex+1));
			System.out.println("My port : "+portStr+" My successor port : "+mysuccessor);

		}
		else if (myindex == 4)
		{
			mysuccessor = global_key.get(hash_key.get(0));
			System.out.println("My port : "+portStr+" My successor port : "+mysuccessor);
		}


		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

//		Cursor c = null;
//
//		Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
//		c = query(uri,null,"@",null,null);


		message msg = new message(null,null, portStr, 6, dummy_map);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);

		Log.i(TAG,"Failed port recovered; Client task created");


		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		FileInputStream fileInputStream;
		String[] col = {Key,Value};
		if(selection.equals("@"))
		{
			Log.i(TAG, "In local query method");
			MatrixCursor matrixCursor = new MatrixCursor(col);
			try{
				String[] files = getContext().fileList();
				for (String f : files)
				{
					fileInputStream = getContext().openFileInput(f);
					BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
					String val = br.readLine();
					System.out.println(val);
					matrixCursor.addRow(new String[] {f,val});
					Log.i(TAG,"Querying : "+f);
				}
			} catch (FileNotFoundException e) {
				Log.e(TAG, "File not found");
			} catch (IOException e) {
				Log.e(TAG, "I/O Exception");
			}

			return matrixCursor;

		}
		else if(selection.equals("*"))
		{
			Log.i(TAG, "In global query method");
			MatrixCursor matrixCursor = new MatrixCursor(col);
			try{
				String[] files = getContext().fileList();
				for (String f : files)
				{
					fileInputStream = getContext().openFileInput(f);
					BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
					String val = br.readLine();
					System.out.println(val);
					matrixCursor.addRow(new String[] {f,val});
					Log.i(TAG,"Querying : "+f);
				}
			} catch (FileNotFoundException e) {
				Log.e(TAG, "File not found");
			} catch (IOException e) {
				Log.e(TAG, "I/O Exception");
			}

			for(String p : ports)
			{
				if(!p.equals(myPort))
				{
					Socket socket = null;
					try {
						message m = new message(null, null, String.valueOf(Integer.parseInt(myPort)/2), 1, dummy_map);
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p));
						Log.i(TAG, "Sending to " + p);
						OutputStream outputStream = socket.getOutputStream();
						ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
						objectOutputStream.writeObject(m);

						ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
						message msg1 = (message) in.readObject();

						for (Map.Entry<String, String> entry : msg1.key_val.entrySet())
						{
							Log.i(TAG, "Adding all key value pairs from "+msg1.originalport);
							String k = entry.getKey();
							String v = entry.getValue();
							matrixCursor.addRow(new String[] {k,v});
						}

					}catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			}

			return matrixCursor;

		}
		else
		{
			Log.i(TAG, "In selective query method");
			MatrixCursor matrixCursor = new MatrixCursor(col);

			String hashed_selection=null;
			try {
				hashed_selection = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

			String queryPort = null;
			int queryIndex = -1;

			Collections.sort(hash_key);
			for(String k : hash_key)
			{
				if(hashed_selection.compareTo(k)<0)
				{
					queryPort = global_key.get(k);
					queryIndex = hash_key.indexOf(k);
					break;
				}
			}

			if (queryPort == null)
			{
				Log.i(TAG,"Hashed key greater than all keys");
				queryPort = global_key.get(hash_key.get(0));
				queryIndex = 0;
			}

			String successor1 = null;
			String successor2 = null;


			if(queryIndex<=2)
			{
				successor1 = global_key.get(hash_key.get(queryIndex+1));
				successor2 = global_key.get(hash_key.get(queryIndex+2));
			}
			else if (queryIndex==3)
			{
				successor1 = global_key.get(hash_key.get(4));
				successor2 = global_key.get(hash_key.get(0));
			}
			else
			{
				successor1 = global_key.get(hash_key.get(0));
				successor2 = global_key.get(hash_key.get(1));
			}

			Log.i(TAG, "Querying key in ports "+queryPort+" "+successor1+" "+successor2);

			String[] query_ports = {queryPort, successor1, successor2};

			List<String> values = new ArrayList<String>();

			for (String p : query_ports)
			{
				Log.i(TAG, "Sending message to relevant ports");
				Socket socket = null;
				try {
					message m = new message(selection, null, String.valueOf(Integer.parseInt(myPort)/2), 2, dummy_map);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(p)*2);
					Log.i(TAG, "Sending to " + p);
					OutputStream outputStream = socket.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(m);

					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
					message msg1 = (message) in.readObject();

					for (Map.Entry<String, String> entry : msg1.key_val.entrySet())
					{
						Log.i(TAG, "Adding all key value pairs from "+msg1.originalport);
						String k = entry.getKey();
						String v = entry.getValue();
						values.add(v);
					}


				}catch (IOException e) {
					Log.e(TAG,"IOException caught at query");
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			}

			if (values.size() == 3)
			{
				Map<String,Integer> valCountMap = new HashMap<String,Integer>();  // maintain a hashmap of different values for keys and their corresponding counts. Majority should be >=2

				for(int i=0;i<values.size();i++)
				{
					if(valCountMap.containsKey(values.get(i)))
					{
						int value = valCountMap.get(values.get(i));
						value++;
						valCountMap.put(values.get(i),value);
						Log.i("Local Map values",values.get(i)+" "+value);
					}
					else
					{
						valCountMap.put(values.get(i),1);
						Log.i("Local Map values",values.get(i)+" One");
					}
				}

				for(String key:valCountMap.keySet())
				{
					int val=valCountMap.get(key);
					if(val>=2)                         // Quorum majority
					{
						if(key.equalsIgnoreCase(values.get(0)))
						{
							Log.i(TAG,"Majority found 1");
							matrixCursor.addRow(new String[] {selection,values.get(0)});
							break;
						}
						else if(key.equalsIgnoreCase(values.get(1)))
						{
							Log.i(TAG,"Majority found 2");
							matrixCursor.addRow(new String[] {selection,values.get(1)});
							break;
						}
						else if(key.equalsIgnoreCase(values.get(2)))
						{
							Log.i(TAG,"Majority found 3");
							matrixCursor.addRow(new String[] {selection,values.get(1)});
							break;
						}
					}
					else            // default key-val to consider would be that of the original AVD
					{
						matrixCursor.addRow(new String[] {selection,values.get(0)});
						Log.i(TAG,"No majority found");
					}
				}

			}

			else
			{
				System.out.println(values.size());
				if(values.get(0).equals(values.get(1)))
				{
					Log.i(TAG,"Failed port query; if condition");
					matrixCursor.addRow(new String[] {selection,values.get(0)});
				}
				else            // default key-val to consider would be that of the original AVD
				{
					matrixCursor.addRow(new String[] {selection,values.get(1)});
					Log.i(TAG,"Default value sent");
				}
			}

			return matrixCursor;

		}
	}

	public String queryVal(Uri uri, String key)
	{
		Log.i(TAG, "In queryVal method");
		FileInputStream fileInputStream;
		String[] col = {Key,Value};
		String val = null;
		try{
			fileInputStream = getContext().openFileInput(key);
			BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
			val = br.readLine();
			System.out.println(val);

		} catch (FileNotFoundException e) {
			Log.e(TAG, "File not found");
		} catch (IOException e) {
			Log.e(TAG, "I/O Exception");
		}
		return val;
	}


	private class ClientTask extends AsyncTask<message, message, Void> {

		@Override
		protected Void doInBackground(message... msgs) {
			message obj = msgs[0];
			Socket socket = null;

			if(obj.op_type == 6)
			{
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mysuccessor)*2);
					Log.i(TAG, "Sending recovery msg to " + mysuccessor);
					OutputStream outputStream = socket.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(obj);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			else if(obj.op_type == 7)
			{
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(obj.originalport)*2);
					Log.i(TAG, "Sending recovered insert msgs to " + obj.originalport);
					OutputStream outputStream = socket.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(obj);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			else if(obj.op_type == 9)
			{
				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(obj.originalport)*2);
					Log.i(TAG, "Sending recovered delete msgs to " + obj.originalport);
					OutputStream outputStream = socket.getOutputStream();
					ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
					objectOutputStream.writeObject(obj);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			return null;
		}


	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
