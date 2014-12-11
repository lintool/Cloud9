package edu.umd.hooka.ttables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.FloatBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import edu.umd.hooka.alignment.IndexedFloatArray;


/**
 * Data structure that stores translation probabilities p(f|e) or
 * counts c(f,e).  The set of values (*,e) associated with a particular
 * e are stored adjacent to one another in an array.  For a given f
 * in (f,e) the location of the f is found using a binary search.
 * 
 * Layout: http://www.umiacs.umd.edu/~redpony/ttable-structure.png
 * 
 * @author redpony
 *
 */
public class TTable_monolithic extends TTable implements Cloneable {

	int[] _ef;                       // length <= |E|x|F|
	int[] _e;                        // length = |E|
	float[] _values;
	IndexedFloatArray _nullValues;   // length = |F|
	Path _datapath;
	FileSystem _fs;
	
	private static final Logger myLogger = Logger.getLogger(TTable_monolithic.class);
	
	int eLen;
	int indexLen;
	
	public Object clone() {
		TTable_monolithic res = new TTable_monolithic();
		res._ef = _ef.clone();
		res._e = _e.clone();
		res._values = _values.clone();
		res.eLen = eLen;
		res.indexLen = indexLen;
		res._nullValues = (IndexedFloatArray)_nullValues.clone();
		return res;
	}
	
	public TTable_monolithic() {}
	public TTable_monolithic(FileSystem fs, Path p) throws IOException {
		_fs = fs; _datapath = p;
		this.readFields(_fs.open(_datapath));
	}

	public TTable_monolithic(int[] e, int[] ef, int maxF) {
		_ef = ef;
		_e = e;
		_nullValues = new IndexedFloatArray(maxF + 1);
		eLen = _e.length;
		indexLen = _ef.length;
		_values = new float[indexLen];
	}

	public TTable_monolithic(int[] e, int[] ef, int maxF, FileSystem fs, Path p) {
		_ef = ef;
		_e = e;
		_nullValues = new IndexedFloatArray(maxF + 1);
		eLen = _e.length;
		indexLen = _ef.length;
		_values = new float[indexLen];
		_fs = fs;
		_datapath = p;
	}
	
	public int getMaxF() {
		return _nullValues.size() - 1;
	} 
	
	public int getMaxE() {
		return _e.length - 1;
	}
	
	int binSearch(int e, int f) {
		int min = _e[e];
		int max = _e[e+1] - 1;
		while (min <= max) {
			int mid = (min + max) / 2;
			if (_ef[mid] > f)
				max = mid - 1;
			else if (_ef[mid] < f)
				min = mid + 1;
			else
				return mid;
		}
		throw new RuntimeException("Couldn't find (" + f + "," + e +")");
	}
		
	public void add(int e, int f, float delta) {
		if (e == 0)
			_nullValues.add(f, delta);
		else
			_values[binSearch(e,f)] += delta;
	}
	
	public void set(int e, int f, float value) {
		if (e == 0)
			_nullValues.set(f, value);
		else
			_values[binSearch(e,f)] = value;		
	}
	
	public void setDistribution(float[] x) {
		_values = x.clone();
	}
	
	public void setNullDistribution(float[] x) {
		myLogger.setLevel(Level.DEBUG);
		myLogger.debug("Length of input array is " + x.length);
		_nullValues = new IndexedFloatArray(x.length);
		_nullValues.set(0, 0.0f);
		for(int i=1; i<x.length; i++)
			_nullValues.set(i, x[i]);
	}
	
	/*
	public void setNullDistribution(float[] x) {
		_nullValues = (IndexedFloatArray)(x.clone());
	}
	*/

	public void set(int e, IndexedFloatArray fs) {
		if (e == 0) {
			_nullValues.copyFrom(fs);
			return;
		}
		int from = _e[e];
		int len = _e[e+1] - from;
		if (len != fs.size())
			throw new RuntimeException("Mismatch lengths: in ttable there are " + 
					fs.size() + " parameters for e="+e+ ", but in the IA there are " + len);
		fs.copyTo(_values, from);
	}
	
	/*
	public void plusEquals(int e, IndexedFloatArray fs) {
		if (e == 0)
			_nullValues.plusEquals(fs);
		int from = _e[e];
		int len = _e[e+1] - from;
		if (len != fs.size())
			throw new RuntimeException("Mismatch lengths: in ttable there are " + 
					fs.size() + " parameters for e="+e+ ", but in the IA there are " + len);
		fs.addTo(_values, from);
	}*/
	
	public float get(int e, int f) {
		if (e == 0)
			return _nullValues.get(f);
		else {
			int min = _e[e];
			int max = _e[e+1] - 1;
			while (min <= max) {
				int mid = (min + max) / 2;
				if (_ef[mid] > f)
					max = mid - 1;
				else if (_ef[mid] < f)
					min = mid + 1;
				else
					return _values[mid];
			}
			return 0.0f;
		}
	}
	
	public void clear() {
		java.util.Arrays.fill(_values, 0.0f);
	}
	
	public void prune(float threshold) {
		throw new RuntimeException("Not implemented");
	}
	
	public void normalize() {
		_nullValues.normalize();
		for (int e = 1; e<_e.length - 1; e++) {
			int bf = _e[e];
			int ef = _e[e+1];
			float total = 0.0f;
			for (int f = bf; f < ef; f++) {
				total += _values[f];
			}
			// make uniform
			if (total == 0.0f) {
				float u = 1.0f/(float)(ef - bf);
				for (int f = bf; f < ef; f++)
					_values[f] = u;
			} else {
				// normalize
				for (int f = bf; f < ef; f++) {
					_values[f] /= total;
				}
			}
		}
	}
		
	public void readFields(DataInput in) throws IOException {
		int bbLen = in.readInt();
		ByteBuffer bb=ByteBuffer.allocate(bbLen);
		in.readFully(bb.array());
		IntBuffer ib = bb.asIntBuffer();
		_e = new int[bbLen/4];
		ib.get(_e);
		eLen = _e.length;

		if (_nullValues == null)
			_nullValues = new IndexedFloatArray();
		_nullValues.readFields(in);
	
		bbLen = in.readInt();
		bb=ByteBuffer.allocate(bbLen);
		in.readFully(bb.array());
		ib = bb.asIntBuffer();
		_ef = new int[bbLen/4];
		ib.get(_ef);
		
		bb=ByteBuffer.allocate(bbLen);
		in.readFully(bb.array());
		FloatBuffer fb = bb.asFloatBuffer();
		_values = new float[bbLen/4];
		fb.get(_values);
		indexLen = _values.length;
	}

	public void write(DataOutput out) throws IOException {
		int bbLen = eLen * 4;
		out.writeInt(bbLen);
		ByteBuffer bb=ByteBuffer.allocate(bbLen);
		IntBuffer ib = bb.asIntBuffer();
		ib.put(_e, 0, eLen);
		out.write(bb.array());

		_nullValues.write(out);
		
		bbLen = indexLen * 4;
		out.writeInt(bbLen);
		bb=ByteBuffer.allocate(bbLen);
		ib=bb.asIntBuffer();
		ib.put(_ef, 0, indexLen);
		out.write(bb.array());
		
		bb=ByteBuffer.allocate(bbLen);
		FloatBuffer fb=bb.asFloatBuffer();
		fb.put(_values, 0, indexLen);
		out.write(bb.array());
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("NULL: ").append(_nullValues.toString()).append("\n");
		for (int e = 1; e<_e.length - 1; e++) {
			int bfi = _e[e];
			int efi = _e[e+1];
			for (int fi = bfi; fi < efi; fi++) {
				sb.append("e=").append(e)
  				  .append(" f=").append(_ef[fi]).append(" val=").append(_values[fi]).append("\n");
			}
		}
		return sb.toString();
	}

	@Override
	public void write() throws IOException {
		this.write(_fs.create(_datapath));
	}
}
