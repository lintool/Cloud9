package edu.umd.cloud9.example.hits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.ArrayListOfIntsWritable;

public class HITSNode implements Writable {
	public static final int TYPE_HUB_COMPLETE = 1;
	public static final int TYPE_HUB_MASS = 3;
	public static final int TYPE_HUB_STRUCTURE = 5;
	public static final int TYPE_AUTH_COMPLETE = 2;
	public static final int TYPE_AUTH_MASS = 4;
	public static final int TYPE_AUTH_STRUCTURE = 6;
	
	private int mType;
	private int mNodeId;
	private float mHARank;
	private ArrayListOfIntsWritable mAdjacencyList;
	
	public HITSNode() {
	}

	public float getHARank() {
		return mHARank;
	}

	public void setHARank(float r) {
		mHARank = r;
	}

	public int getNodeId() {
		return mNodeId;
	}

	public void setNodeId(int n) {
		mNodeId = n;
	}

	public ArrayListOfIntsWritable getAdjacencyList() {
		return mAdjacencyList;
	}
	
	public void setAdjacencyList(ArrayListOfIntsWritable l) {
		mAdjacencyList = l;
	}

	public int getType() {
		return mType;
	}
	
	public void setType(int type) {
		if (type != TYPE_HUB_COMPLETE && type != TYPE_HUB_MASS && type != TYPE_HUB_STRUCTURE
				&& type != TYPE_AUTH_COMPLETE && type != TYPE_AUTH_MASS && type != TYPE_AUTH_STRUCTURE)
			return;

		mType = type;
	}
	
	/**
	 * Deserializes this object.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		mType = in.readByte();

		mNodeId = in.readInt();

		if (mType == TYPE_HUB_MASS || mType == TYPE_AUTH_MASS) {
			mHARank = in.readFloat();
			return;
		}

		if (mType == TYPE_HUB_COMPLETE || mType == TYPE_AUTH_COMPLETE) {
			mHARank = in.readFloat();
		}

		mAdjacencyList = new ArrayListOfIntsWritable();
		mAdjacencyList.readFields(in);
	}
	
	/**
	 * Serializes this object.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		out.writeByte((byte) mType);
		out.writeInt(mNodeId);

		if (mType == TYPE_HUB_MASS || mType == TYPE_AUTH_MASS) {
			out.writeFloat(mHARank);
			return;
		}

		if (mType == TYPE_HUB_COMPLETE || mType == TYPE_AUTH_COMPLETE) {
			out.writeFloat(mHARank);
		}

		mAdjacencyList.write(out);
	}
	
	public String toString() {
		StringBuilder s = new StringBuilder();

		s.append("{");
		s.append(mNodeId);
		s.append(" ");
		if (mType == TYPE_HUB_COMPLETE || mType == TYPE_HUB_MASS || mType == TYPE_HUB_STRUCTURE)
		{
			s.append("H");
		}
		else if (mType == TYPE_AUTH_COMPLETE || mType == TYPE_AUTH_MASS || mType == TYPE_AUTH_STRUCTURE)
		{
			s.append("A");
		}
		else
		{
			s.append("?");
		}
		s.append(" ");
		s.append(mHARank);
		s.append(" ");

		if (mAdjacencyList == null) {
			s.append("{}");
		} else {
			s.append("{");
			for (int i = 0; i < mAdjacencyList.size(); i++) {
				s.append(mAdjacencyList.get(i));
				if (i < mAdjacencyList.size() - 1)
					s.append(", ");
			}
			s.append("}");
		}

		s.append(" }");

		return s.toString();
	}

}
