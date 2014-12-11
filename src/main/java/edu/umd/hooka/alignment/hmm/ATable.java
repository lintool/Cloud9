package edu.umd.hooka.alignment.hmm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

import org.apache.hadoop.io.Writable;

public class ATable implements Writable, Cloneable {

	float[][] data;
	float _nullTrans;
	int maxDist;
	boolean modelNull;
	int extraIPrevFactors;
	int extraIFactors;
	boolean homogeneous = true;
	
	public Object clone() {
		ATable r = new ATable(homogeneous, data.length, maxDist);
		r._nullTrans = _nullTrans;
		for (int i=0; i < data.length; ++i) {
		  System.arraycopy(data[i], 0, r.data[i], 0, data[i].length);
		}
		return r;
	}
	
	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#clear()
	 */
	public void clear() {
		for (int i = 0; i < data.length; i++) {
			float[] row = data[i];
			for (int j = 0; j < row.length; ++j)
			  row[j] = 0.0f;
		}
		_nullTrans = 0.0f;
	}
	
	public ATable() {}
	
	public ATable(boolean homo, int conditioning_values, int dist) {
		maxDist = dist;
		homogeneous = homo;
		modelNull = false;
		extraIPrevFactors = 0;
		extraIFactors = 0;
		if (homogeneous)
			assert(conditioning_values == 1);
		data = new float[conditioning_values][];
		for (int i = 0; i < conditioning_values; ++i)
			if (homogeneous)
				data[i] = new float[maxDist * 2 + 1];
			else
				data[i] = new float[i * 2 + 1];
	}
	
	public final int getMaxDist() {
		return maxDist;
	}
	
	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#get(int, char)
	 */
	public float get(int jump, char condition) {
		if (jump == -1000) return _nullTrans;
		try {
			if (homogeneous)
				return data[0][jump + maxDist];
			else
				return data[condition][jump + condition];
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			throw new RuntimeException("Tried access: " + jump + "+" +maxDist + " but dl=" + data.length + "  Caught " + e);
		}
	}

	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#get(int, char, int)
	 */
	public float get(int jump, char condition, int dummy) {
		if (jump == -1000) return _nullTrans;
		try {
			if (homogeneous)
				return data[0][jump];
			else
				return data[condition][jump];
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			return 0;
	//		throw new RuntimeException("Tried access: " + jump + "+" 
	//				+ maxDist + " but dl=" + data.length + "  Caught " + e);
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#add(int, char, int, float)
	 */
	public void add(int jump, char condition, int dummy, float v) {
		if (v == 0.0f) return;
		if (jump == -1000)
			_nullTrans += v;
		else {
			if (homogeneous)
				data[0][jump + maxDist] += v;
			else
				data[condition][jump + condition] += v;
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#add(int, char, float)
	 */
	public void add(int coord, char condition, float v)
	{
		if (v == 0.0f) return;
		if (coord == -1000)
			_nullTrans += v;
		else {
			if (homogeneous)
				data[0][coord] += v;
			else
				data[condition][coord] += v;
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#getCoord(int, char)
	 */
	public int getCoord(int jump, char condition) {
		if (homogeneous) {
			if (jump == -1000) return jump;
			return jump + maxDist;
		} else {
			// TODO fix
			if (jump == -1000) return jump;
			return jump + condition;			
		}
	}
	
	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#plusEquals(edu.umd.hooka.alignment.hmm.ATable)
	 */
	public void plusEquals(ATable rhs) {
		if (data.length != rhs.data.length)
			throw new RuntimeException("mismatch lengths!");
		for (int i = 0; i < data.length; i++) {
			float[] row = data[i];
			float[] orow = rhs.data[i];
			assert(row.length == orow.length);
			for (int j = 0; j < row.length; ++j)
				row[j] += orow[j];
		}
		_nullTrans += rhs._nullTrans;
	}
	
	// TODO: take alpha as a parameter, add support for VB
	/* (non-Javadoc)
	 * @see edu.umd.hooka.alignment.hmm.IATable#normalize()
	 */
	public void normalize() {
		boolean smooth = true;
		float alpha = 0.00001f;
		boolean renorm = false;
		for (float[] row : data) {
			float sum = 0;
			if (modelNull)
				sum = _nullTrans;
			for (float v : row) sum += v;
			if (sum > 0.0f) {
				if (smooth) {
					sum += alpha * (float)(row.length+1);
					if (modelNull)
						_nullTrans = (_nullTrans + alpha) / sum;
					else
						_nullTrans = 0;
					for (int i = 0; i < row.length; i++)
						row[i] = (row[i] + alpha) / sum;				
				} else {
					_nullTrans /= sum;
					for (int i = 0; i < data.length; i++)
						row[i] /= sum;
				}
				continue;
			}
			boolean initializeUniform = false;
			renorm = true;
			if (initializeUniform) {
				float up = 1.0f / (float)(row.length + 1);
				for (int i = 0; i < row.length; i++) {
					row[i] = up;
				}
				if (modelNull)
				    _nullTrans = up;
				else
					_nullTrans = 0;
			} else {
				for (int i = 0; i < row.length; i++) {
					int len = (row.length - 1) / 2;
					int ad = 0;
					if (homogeneous)
						ad = (i-maxDist)-1;
					else
						ad = (i - len) - 1;
					if (ad > 0) ad *= -1;
					if (homogeneous) {
						if (i - maxDist == 0) ad -= 3;
					} else {
						if (i - len == 0 && i > 3) ad -= 3;
					}
					row[i] = (float)Math.exp((double)ad * 0.15);
				}
				if (modelNull & !homogeneous)
					throw new RuntimeException("Not implemented properly");
				if (modelNull) _nullTrans = (float)row[row.length/2]; else
					_nullTrans = 0;
			}
		}
		if (renorm) normalize();
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("ATable: maxDist=").append(maxDist).append('\n');
		int i = -1;
		for (float[] row : data) {
			i++;
			sb.append("cond=").append(i);
			sb.append("     NULL-trans=").append(_nullTrans).append('\n');
			int md = i;
			if (homogeneous)
				md = maxDist;
			for (int j=0; j<row.length; j++)
				sb.append(" P(J=").append(j-md).append(") = ").append(row[j]).append('\t');
			sb.append('\n');
		}
		return sb.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		homogeneous = in.readBoolean();
		maxDist = in.readInt();
		data = new float[in.readInt()][];
		for (int i = 0; i < data.length; ++i) {
			int bbLen = in.readInt();
			ByteBuffer bb=ByteBuffer.allocate(bbLen);
			in.readFully(bb.array());
			FloatBuffer fb = bb.asFloatBuffer();
			data[i] = new float[bbLen/4];
			fb.get(data[i]);
		}
		_nullTrans = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(homogeneous);
		out.writeInt(maxDist);
		out.writeInt(data.length);
		for (int i = 0; i < data.length; ++i) {
			int bbLen = data[i].length * 4;
			out.writeInt(bbLen);
			ByteBuffer bb=ByteBuffer.allocate(bbLen);
			FloatBuffer fb = bb.asFloatBuffer();
			fb.put(data[i], 0, data[i].length);
			out.write(bb.array());
		}
		out.writeFloat(_nullTrans);
	}

}
