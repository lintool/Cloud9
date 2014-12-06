package edu.umd.hooka.alignment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.pair.PairOfFloatInt;
import edu.umd.cloud9.util.array.ArrayListOfInts;


/*
 * Represents a sparse float array.  That is, some indices don't exist.
 * 
 * TODO: performance enhancement, when _data.length > |V|/2 it becomes both more
 * memory efficient and run-time efficient to store a non-sparse array.
 */
public final class IndexedFloatArray implements Writable, Cloneable {
	
	/**
	 * If the sparse array exceeds this threshold, make the array non-sparse.
	 */
	public static final float NO_BINSEARCH_THRESHOLD = 0.90f;
	
	/**
	 * Don't make arrays sparse unless they exceed this length.
	 */
	public static final int MIN_LENGTH_FOR_NONSPARSE_ARRAY = 5;
	
	public float[] _data;
	public int[] _indices;
	public boolean _useBinSearch;

	public void readFields(DataInput in) throws IOException {
		int bbLen = in.readInt();
		if (bbLen == 0) { _data = null; _indices = null; return; }
		ByteBuffer bb=ByteBuffer.allocate(bbLen);
		_useBinSearch = in.readBoolean();
		if (_useBinSearch) {
			in.readFully(bb.array());
			_indices = new int[bbLen/4];
			IntBuffer ib = bb.asIntBuffer();
			ib.get(_indices);
			bb=ByteBuffer.allocate(bbLen);
		}
		in.readFully(bb.array());
		FloatBuffer fb = bb.asFloatBuffer();
		_data = new float[bbLen/4];
		fb.get(_data);
	}

	public void write(DataOutput out) throws IOException {
		if (_data == null) {
			out.writeInt(0);
		} else {
			int bbLen = _data.length * 4;
			out.writeInt(bbLen);
			out.writeBoolean(_useBinSearch);
			ByteBuffer bb=ByteBuffer.allocate(bbLen);
			if (_useBinSearch) {
				IntBuffer ib = bb.asIntBuffer();
				ib.put(_indices);
				out.write(bb.array());
				bb=ByteBuffer.allocate(bbLen);
			}
			FloatBuffer fb = bb.asFloatBuffer();
			fb.put(_data);
			out.write(bb.array());
		}
	}
	
	public Object clone() {
		IndexedFloatArray res = new IndexedFloatArray();
		if (_data == null) { return res; }
		res._data = _data.clone();
		res._useBinSearch = _useBinSearch;
		if (_useBinSearch)
			res._indices = _indices.clone();
		return res;
	}
	
	public int maxKey() {
		if (_useBinSearch)
			return _indices[_indices.length - 1];
		else
			return _data.length - 1;
	}
	
	private void optimizeMemory(float[] data, int max) {
		if (_useBinSearch) return;
		int nzc = 0;
		for (int c = 0; c < max; c++)
			if (data[c] != 0.0f) nzc++;
		if (nzc == 0) {
			_data = null; _indices = null;
			return;
		}
		float[] nd = new float[nzc];
		int[]   ni = new int[nzc];
		int ci = 0;
		for (int c = 0; c < max; c++) {
			float v = data[c];
			if (v != 0.0f) {
				nd[ci] = v;
				ni[ci] = c;
				ci++;
			}
		}
		_data = nd;
		_indices = ni;
		_useBinSearch = true;
	}

	/**
	 * If sparse array meets the load criteria, optimize it so that it no longer uses
	 * a bin search.
	 */
	public void optimizeSpeed() {
		if (_indices == null || _indices.length < MIN_LENGTH_FOR_NONSPARSE_ARRAY) return;
		int maxIndex = _indices[_indices.length - 1];
		float load = ((float)_data.length)/((float)maxIndex);
		if (load > NO_BINSEARCH_THRESHOLD) {
			System.err.println("Optimizing IFA: len=" + _indices.length + ", load=" 
					+ load +", newMax=" + maxIndex);
			float[] nd = new float[maxIndex+1];
			for (int i = 0; i < _indices.length; i++)
				nd[_indices[i]] = _data[i];
			_data = nd;
			_indices = null;
			_useBinSearch = false;
		}
	}
	
	public void copyTo(float[] dest, int destPos) {
		System.arraycopy(_data, 0, dest, destPos, _data.length);
	}
	
	public void copyFrom(IndexedFloatArray rhs) {
		System.arraycopy(rhs._data, 0, _data, 0, _data.length);
	}
	public void addTo(float[] dest) {
		if (_useBinSearch) {
			for (int i = 0; i < _data.length; i++)
				dest[_indices[i]] += _data[i];
		} else {
			for (int i = 0; i < _data.length; i++)
				dest[i] += _data[i];
		}
	}

	public IndexedFloatArray() {}
	public IndexedFloatArray(int[] indices, float[] values) {
		_indices = indices;
		_data = values;
		_useBinSearch = true;
		optimizeSpeed();
	}
	public IndexedFloatArray(int[] indices, float[] values, boolean isOptimize) {
		_indices = indices;
		_data = values;
		_useBinSearch = true;
		if(isOptimize)
			optimizeSpeed();
	}
	public IndexedFloatArray(float[] values, int size) {
		_useBinSearch = false;
		int nzc = 0;
		for (int i=0; i<values.length; i++)
			if (values[i] != 0.0f) nzc++;
		if (nzc == 0) { _data = null; _indices = null; return; }
		float load = ((float)nzc)/((float)size);
		if (size < MIN_LENGTH_FOR_NONSPARSE_ARRAY ||
				load <= NO_BINSEARCH_THRESHOLD) {
			optimizeMemory(values, size);
		} else {
			_indices = null;
			_data = new float[size];
			System.arraycopy(values, 0, _data, 0, size);
		}
	}
	public IndexedFloatArray(int[] indices) {
		_indices = indices.clone();
		_data = new float[_indices.length];
		_useBinSearch = true;
	}
	// TODO: in this case, make this a single lookup type data structure,
	// ie, skip the bin search. Normally would use polymorphism for this,
	// but hadoop's SequenceFiles don't like that kind of polymorphism
	public IndexedFloatArray(int n) {
		_indices = null;
		_useBinSearch = false;
		_data = new float[n];
	}

	final int binSearch(int n) {
		if (!_useBinSearch) return n;
		int min = 0;
		int max = _indices.length - 1;
		while (min <= max) {
			int mid = (min + max) / 2;
			if (_indices[mid] > n)
				max = mid - 1;
			else if (_indices[mid] < n)
				min = mid + 1;
			else
				return mid;
		}
		throw new RuntimeException("IFA: Couldn't find " + n);
	}

	public int size() {
		if (_data != null) return _data.length;
		else return 0;
	}

	
	public int getWord(int loc){
		return _indices[loc];
	}
	
	public float getProb(int loc){
		return _data[loc];
	}
	
	//Ferhan: i don't know what this is doing. the behavior tends to be dependent on _useBinSearch value
	public final float get(int n) {
		if (_data == null) return 0.0f;
		if (!_useBinSearch) if (n >= _data.length) return 0.0f; else return _data[n];
		int min = 0;
		int max = _indices.length - 1;
		while (min <= max) {
			int mid = (min + max) / 2;
			if (_indices[mid] > n)
				max = mid - 1;
			else if (_indices[mid] < n)
				min = mid + 1;
			else
				return _data[mid];
		}
		return 0.0f;
	}
	
	public final float getLazy(int n) {
		if (_data == null) return 0.0f;
		for(int i=0; i<_indices.length; i++){
			if(_indices[i] == n){
				return _data[i];
			}
		}
		return 0.0f;
	}
	
	public int[] getTranslations(float probThreshold){
		ArrayListOfInts words = new ArrayListOfInts(); 
		if (_useBinSearch) {
      for (int i=0; i < _data.length; i++) {
        if (_data[i] > probThreshold) {
					words.add(_indices[i]);
				}
			}
		}else{
      for (int i=0; i < _data.length; i++) {
        if (_data[i] > probThreshold) {
					words.add(i);
				}
			}
		}
		words.trimToSize();
		return words.getArray();
	}
	
	public PriorityQueue<PairOfFloatInt> getTranslationsWithProbs(float probThreshold){
		PriorityQueue<PairOfFloatInt> q = new PriorityQueue<PairOfFloatInt>(_data.length, Collections.reverseOrder()); 
    if (_useBinSearch) {
      for (int i=0; i < _data.length; i++) {
        if (_data[i] > probThreshold) {
					q.add(new PairOfFloatInt(_data[i],_indices[i]));
				}
			}
		}else{
      for (int i=0; i < _data.length; i++) {
        if (_data[i] > probThreshold) {
					q.add(new PairOfFloatInt(_data[i],i));
				}
			}
		}
		return q;
	}
	
	public List<PairOfFloatInt> getTranslationsWithProbsAsList(float probThreshold){
		List<PairOfFloatInt> l = new ArrayList<PairOfFloatInt>(); 
    if (_useBinSearch) {
			for(int i=0; i < _data.length; i++){
				if (_data[i] > probThreshold) {
					l.add(new PairOfFloatInt(_data[i],_indices[i]));
				}
			}
		}else{
			for (int i=0; i < _data.length; i++) {
				if (_data[i] > probThreshold) {
					l.add(new PairOfFloatInt(_data[i],i));
				}
			}
		}
		return l;
	}
	
	public final void set(int index, float value) { _data[binSearch(index)] = value; }
	public final void add(int index, float delta) { _data[binSearch(index)]+= delta; }
	
	/**
	 * @param index
	 * 		the index of the searched term
	 * @return
	 * 		the location of the term in the array
	 */
	public int getAddr(int index) { return binSearch(index); }
	public void clear() {
		int l = size();
		for (int i=0; i<l; i++)
			_data[i]=0.0f;
	}
	public void plusEqualsMismatchSize(IndexedFloatArray rhs) {
		if (this._data == null) {
			if (rhs._data == null) return;
			this._data = rhs._data.clone();
			if (rhs._indices != null)
				this._indices = rhs._indices.clone();
			this._useBinSearch = rhs._useBinSearch;
			return;
		}
		this.optimizeMemory(_data, _data.length);
		rhs.optimizeMemory(rhs._data, rhs._data.length);
		float[] tv = new float[_data.length + rhs._data.length];
		int[] tk = new int[_data.length + rhs._data.length];
		int cl = 0;
		int cr = 0;
		int c = 0;
		while(cl < _data.length && cr < rhs._data.length) {
			int il = _indices[cl];
			int ir = rhs._indices[cr];
			if (il == ir) {
				tk[c] = ir;
				tv[c] = _data[cl] + rhs._data[cr];
				cr++; cl++;
			} else if (il < ir) {
				tk[c] = il;
				tv[c] = _data[cl];
				cl++;
			} else {
				tk[c] = ir;
				tv[c] = rhs._data[cr];
				cr++;
			}
			c++;
		}
		if (cl < _data.length) {
			int dif = _data.length - cl;
			System.arraycopy(_data, cl, tv, c, dif);
			System.arraycopy(_indices, cl, tk, c, dif);
			c += dif;
		} else if (cr < rhs._data.length) {
			int dif = rhs._data.length - cr;
			System.arraycopy(rhs._data, cr, tv, c, dif);
			System.arraycopy(rhs._indices, cr, tk, c, dif);
			c += dif;				
		}
		if (c == tv.length) {
			_data = tv;
			_indices = tk;
		} else {
			int[] ni = new int[c];
			float[] nv = new float[c];
			System.arraycopy(tk, 0, ni, 0, c);
			System.arraycopy(tv, 0, nv, 0, c);
			_data = nv;
			_indices = ni;
			this.optimizeSpeed();
		}
	}
	public void plusEquals(IndexedFloatArray rhs) {
		if (size() != rhs.size())
			throw new RuntimeException("Size mismatch");
		if (size() == 0) return;
		for (int i=0; i<_data.length; i++)
			_data[i] += rhs._data[i];
	}
	public void minusEquals(IndexedFloatArray rhs) {
		if (size() != rhs.size())
			throw new RuntimeException("Size mismatch");
		if (size() == 0) return;
		for (int i=0; i<_data.length; i++)
			_data[i] -= rhs._data[i];
	}
	public void timesEquals(float rhs) {
		if (size() == 0) return;
		for (int i=0; i<_data.length; i++)
			_data[i] *= rhs;
	}
	public void normalize() {
		normalize(0.0f);
	}
	public void normalize(float alpha) {
		if (size() == 0) return;
		float total = 0.0f;
		for (float f: _data)
			total += (f  + alpha);
		if (total == 0.0f) {
			float v = 1.0f / (float)size();
			for (int i=0; i<_data.length; i++)
				_data[i] = v;
		} else {
			for (int i=0; i<_data.length; i++)
				_data[i] = (_data[i] + alpha) / total;
		}
	}
	public void normalize_variationalBayes(float alpha) {
		if (size() == 0) return;
		float total = 0.0f;
		for (float f: _data)
			total += (f + alpha);
		if (total == 0.0f) {
			if (true) throw new RuntimeException("Sum=0: shouldn't happen " + this);
			float v = 1.0f / (float)size();
			for (int i=0; i<_data.length; i++)
				_data[i] = v;
		} else {
			for (int i=0; i<_data.length; i++)
				_data[i] = (float)Math.exp(Digamma.digamma(_data[i] + alpha) - Digamma.digamma(total));
		}		
	}
	public float innerProduct(IndexedFloatArray rhs) {
		if (size() != rhs.size())
			throw new RuntimeException("Size mismatch");
		if (size() == 0) return 0.0f;
		float res = 0.0f;
		for (int i=0; i<_data.length; i++)
			res += _data[i] * rhs._data[i];
		return res;
	}
	public String toString(boolean brackets) {
		StringBuffer sb = new StringBuffer();

		if (brackets) sb.append('<');
		if (_data == null)
			sb.append("null");
		else {
			if (_useBinSearch) {
				if (size() > 0) {
					for (int i=0; i<_data.length; i++) {
						if (i != 0) sb.append(' ');
						sb.append(_indices[i]+":"+_data[i]);
					}
				}
			} else {
				for (int i=0; i<_data.length; i++) {
					if (i != 0) sb.append(' ');
					sb.append(i+":"+_data[i]);
				}
			}
		}
		if (brackets) sb.append('>');
		return sb.toString();		
	}
	public String toString() {
		return toString(true);
	}



}
