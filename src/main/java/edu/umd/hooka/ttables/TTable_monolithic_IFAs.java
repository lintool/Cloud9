package edu.umd.hooka.ttables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
 */
public class TTable_monolithic_IFAs extends TTable implements Cloneable {

	IndexedFloatArray[] _data;
	Path _datapath;
	FileSystem _fs;
	
	int eLen;
	int indexLen;
	
	public Object clone() {
		TTable_monolithic_IFAs res = new TTable_monolithic_IFAs();
		res._data = new IndexedFloatArray[_data.length];
		for (int i=0; i<_data.length; i++)
			res._data[i] = (IndexedFloatArray)_data[i].clone();
		res.eLen = eLen;
		res.indexLen = indexLen;
		return res;
	}
	
	public TTable_monolithic_IFAs() {
		_data = new IndexedFloatArray[10];
	}

	public TTable_monolithic_IFAs(FileSystem fs, Path p, boolean load) throws IOException {
		_fs = fs; _datapath = p;
		if (load)
			this.readFields(_fs.open(_datapath));
		else
			_data = new IndexedFloatArray[10];
	}
	
	public int getMaxF() {
		return _data[0].size() - 1;
	} 
	
	public int getMaxE() {
		return _data.length - 1;
	}

	private final void ensureSize(int e) {
		if (e >= _data.length) {
			IndexedFloatArray[] x = new IndexedFloatArray[(int)(((float)e)*1.5f)];
			System.arraycopy(_data, 0, x, 0, _data.length);
			_data = x;
		}
	}
	public void add(int e, int f, float delta) {
		ensureSize(e);
		_data[e].add(f, delta);
	}
	
	public void set(int e, int f, float value) {
		// don't ensure size-- IFA won't resize!
		_data[e].set(f, value);
	}
	
	public void set(int e, IndexedFloatArray fs) {
		ensureSize(e);
		if (_data[e] == null) {
			_data[e] = (IndexedFloatArray)fs.clone();
		} else {
			_data[e].copyFrom(fs);
		}
	}
		
	public float get(int e, int f) {
		return _data[e].get(f);
		
	/*	catch (Exception ex) {
			throw new RuntimeException("e=" + e + " f="+f + "\n"+ _data[e] +"\nCaught " + ex);
		}*/
	}
	
	public IndexedFloatArray get(int e) {
		return _data[e];
	/*	catch (Exception ex) {
			throw new RuntimeException("e=" + e + " f="+f + "\n"+ _data[e] +"\nCaught " + ex);
		}*/
	}
	
	public void prune(float threshold) {
		throw new RuntimeException("Not implemented");
	}
	
	public void normalize() {
		for (IndexedFloatArray x : _data) {
			if (x != null) x.normalize();
		}
	}
		
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		_data = new IndexedFloatArray[len];
		for (int i = 0; i < len; i++) {
			_data[i] = new IndexedFloatArray();
			_data[i].readFields(in);
		}
	}

	public void write(DataOutput out) throws IOException {
		int len = 0;
		int max = 0;
		while(len < _data.length) {
			if (_data[len] != null) max = len + 1;
			len++;
		}
		out.writeInt(max);
		IndexedFloatArray nullA = new IndexedFloatArray();
		for (int i = 0; i < max; i++) {
			if (_data[i] == null)
				nullA.write(out);
			else
				_data[i].write(out);
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int e = 0; e< _data.length; e++) {
			sb.append("e=").append(e).append(' ').append(_data[e]).append('\n');
 		}
		return sb.toString();
	}

	@Override
	public void write() throws IOException {
		FSDataOutputStream s = _fs.create(_datapath);
		this.write(s);
		s.close();
	}

	@Override
	public void clear() {
		for (IndexedFloatArray i : _data)
			if (i != null) i.clear();
	}
}
