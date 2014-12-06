package edu.umd.hooka;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import edu.umd.hooka.alignment.IndexedFloatArray;
import edu.umd.hooka.ttables.TTable;

public class PServerClient extends TTable {

	HashMap<String, Integer> map = new HashMap<String, Integer>();
	Socket s;
	DataInputStream is;
	DataOutputStream os;
	static final int BUF_SIZE=300000;
	ByteBuffer bb = ByteBuffer.allocate(BUF_SIZE);
	int entries;
	
	public PServerClient(
			String host,
			int port) throws IOException {
		System.err.println("Connecting to PServer: " + host + ":" + port);
		s = new Socket(host, port);
		is = new DataInputStream(s.getInputStream());
		os = new DataOutputStream(s.getOutputStream());
	}
	
	public void query(PhrasePair pp, boolean nullWord) throws IOException {
		dict.clear();
		int[] es = pp.getE().getWords();
		int[] fs = pp.getF().getWords();
		int size = 4*(es.length + fs.length + 2);
		if (nullWord) size += 4;
		bb.rewind();
		bb.limit(BUF_SIZE);
		bb.putInt(size);
		int elen = es.length;
		if (nullWord) ++elen;
		bb.putInt(elen);
		if (nullWord) bb.putInt(0);
		for (int e : es) bb.putInt(e);
		for (int f : fs) bb.putInt(f);
		bb.flip();
		os.write(bb.array(), 0, size);
		
		bb.rewind();
		bb.limit(BUF_SIZE);
		size = is.readInt();
		is.readFully(bb.array(), 0, size);
		entries=0;
		for (int i = 0; i < elen; ++i) {
			int ew = 0;
			if (nullWord) {
			    if (i>0) ew = es[i-1];
			} else {
				ew = es[i];
			}
			temp.set(ew);
			HashMap<IntWritable,Float> v1 = dict.get(temp);
			if (v1 == null) {
				v1 = new HashMap<IntWritable, Float>(fs.length*2);
				dict.put(temp, v1);
			}
			for (int j = 0; j < fs.length; ++j) {
				temp.set(fs[j]);
				float val = bb.getFloat();
				if (v1.get(temp) == null) {
					v1.put(temp, val);
				}
			}
		}
	}
	
	IntWritable temp = new IntWritable();
	HashMap<IntWritable, HashMap<IntWritable,Float>> dict =
		new HashMap<IntWritable, HashMap<IntWritable,Float>>();

	@Override
	public void add(int e, int f, float delta) {
		throw new UnsupportedOperationException();
	}
	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}
	@Override
	public Object clone() {
		throw new UnsupportedOperationException();
	}
	@Override
	public float get(int e, int f) {
		temp.set(e);
		HashMap<IntWritable, Float> v1 = dict.get(temp);
		if (v1 == null)
			return 0.0f;
		temp.set(f);
		Float v2 = v1.get(temp);
		if (v2 == null)
			return 0.0f;
		return v2.floatValue();
	}
	@Override
	public void normalize() {
		throw new UnsupportedOperationException();
	}
	@Override
	public void set(int e, int f, float value) {
		throw new UnsupportedOperationException();
	}
	@Override
	public void set(int e, IndexedFloatArray fs) {
		throw new UnsupportedOperationException();
	}
	@Override
	public void write() throws IOException {
		throw new UnsupportedOperationException();
	}

}
