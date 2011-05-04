package edu.umd.hooka.ttables;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
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
 *
 */
public class TTable_sliced extends TTable implements Cloneable {

	IndexedFloatArray[] _data;  // length = |F|
	Path _datapath;
	FileSystem _fs;
	boolean cleared = false;
		
	public Object clone() {
		TTable_sliced res = new TTable_sliced();
		res._datapath = _datapath;
		res._fs = _fs;
		res.cleared = cleared;
		res._data = new IndexedFloatArray[_data.length];
		for (int i=0; i<_data.length; i++)
			res._data[i] = (IndexedFloatArray)_data[i].clone();
		return res;
	}
	
	public TTable_sliced() {}
	
	public TTable_sliced(int e_voc_size) {
		_data = new IndexedFloatArray[e_voc_size];
	}

	public TTable_sliced(FileSystem fs, Path p) throws IOException {
		_fs = fs; _datapath = p;
		FSDataInputStream in = _fs.open(_datapath.suffix(Path.SEPARATOR + "metadata.bin"));
		_data = new IndexedFloatArray[in.readInt()];
		in.close();
	}

	public TTable_sliced(int e_voc_size, FileSystem fs, Path p) {
		_fs = fs; _datapath = p;
		_data = new IndexedFloatArray[e_voc_size];
	}
	
	public int getMaxF() {
		return _data[0].size() - 1;
	} 
	
	public int getMaxE() {
		return _data.length - 1;
	}
	
	final private void checkE(int e) {
		if (_data[e] == null) {
			try {
				_data[e] = new IndexedFloatArray();
				FSDataInputStream in = _fs.open(_datapath.suffix(Path.SEPARATOR + "voc_" + e + ".tab"));
				_data[e].readFields(in);
				in.close();
				if (cleared) _data[e].clear();
			} catch (IOException ex) {
				ex.printStackTrace();
				throw new RuntimeException("Caught " + e);
			}
		}
	}
			
	public void add(int e, int f, float delta) {
		checkE(e);
		_data[e].add(f, delta);
	}
	
	public void set(int e, int f, float value) {
		checkE(e);
		_data[e].set(f, value);
	}
	public long getCoord(int e, int f) {
		return ((long)e) << 32 | (long)_data[e].getAddr(f);
	}

	public void add_addr(long coord, float delta) {
		_data[(int)(coord >> 32)].add((int)(coord & 0xffffffffl), delta);
	}

	public void set(int e, IndexedFloatArray fs) {
		if (_data[e] == null) {
			_data[e] = (IndexedFloatArray)fs.clone();
		} else {
			_data[e].copyFrom(fs);
		}
	}
	
	/**
	public void plusEquals(int e, IndexedFloatArray fs) {
		checkE(e);
		_data[e].plusEquals(fs);
	}*/
	
	public float get(int e, int f) {
		checkE(e);
		return _data[e].get(f);
	}

	public void clear() {
		cleared = true;
		for (IndexedFloatArray ar : _data) {
			if (ar != null) ar.clear();
		}
	}
	
	public void prune(float threshold) {
		throw new RuntimeException("Not implemented");
	}
	
	public void normalize() {
		for (IndexedFloatArray ar : _data)
			if (ar != null) ar.normalize();
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
		throw new RuntimeException("Not implemented");
	}

}
