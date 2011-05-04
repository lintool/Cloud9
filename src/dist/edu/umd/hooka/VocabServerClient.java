package edu.umd.hooka;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;

import org.apache.hadoop.io.Text;

public class VocabServerClient implements Vocab {

	HashMap<String, Integer> map = new HashMap<String, Integer>();
	Socket s;
	DataInputStream is;
	DataOutputStream os;
	
	public VocabServerClient(String host, int port) throws IOException {
		System.err.println("Connecting to vocab server: " + host + ":" + port);
		s = new Socket(host, port);
		is = new DataInputStream(s.getInputStream());
		os = new DataOutputStream(s.getOutputStream());
	}
	
	Text t = new Text();
	public int remoteAddOrGet(String word) {
		int res = 0;
		try {
			os.writeByte(Text.utf8Length(word));
			Text.writeString(os, word);
			os.writeByte(0);
			os.flush();
			res = is.readInt();
		} catch (IOException e) {
			throw new RuntimeException("Caught " +e);
		}
		return res;
	}

	public int addOrGet(String word) {
		Integer i = map.get(word);
		if (i == null) {
			int iv = remoteAddOrGet(word);
			i = new Integer(iv);
			map.put(word, i);
		}
		return i.intValue();
	}

	public int get(String word) {
		// TODO Auto-generated method stub
		return 0;
	}

	public String get(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
