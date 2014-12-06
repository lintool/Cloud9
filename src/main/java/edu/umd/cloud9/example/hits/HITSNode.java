package edu.umd.cloud9.example.hits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * 
 * <p>Representation of a Hubs and Authorities Node for HITS algorithm computation</p>
 * <p> lets try git stuff</p>
 * 
 * @author Mike McGrath
 *
 */


public class HITSNode implements Writable {
	public static final int TYPE_HUB_COMPLETE = 1;
	public static final int TYPE_HUB_MASS = 3;
	public static final int TYPE_HUB_STRUCTURE = 5;
	public static final int TYPE_AUTH_COMPLETE = 2;
	public static final int TYPE_AUTH_MASS = 4;
	public static final int TYPE_AUTH_STRUCTURE = 6;
	public static final int TYPE_NODE_COMPLETE = 7;
	public static final int TYPE_NODE_MASS = 8;
	public static final int TYPE_NODE_STRUCTURE = 9;

	private int mType;
	private int mNodeId;
	private float mHRank;
	private float mARank;
	private ArrayListOfIntsWritable mInlinks = new ArrayListOfIntsWritable();
	private ArrayListOfIntsWritable mOutlinks = new ArrayListOfIntsWritable();

	public HITSNode() {
	}

	public float getHRank() {
		return mHRank;
	}
	
	public float getARank() {
		return mARank;
	}

	public void setHRank(float r) {
		mHRank = r;
	}
	
	public void setARank(float r) {
		mARank = r;
	}

	public int getNodeId() {
		return mNodeId;
	}

	public void setNodeId(int n) {
		mNodeId = n;
	}

	public ArrayListOfIntsWritable getInlinks() {
		return mInlinks;
	}

	public void setInlinks(ArrayListOfIntsWritable l) {
		mInlinks = l;
	}
	
	public ArrayListOfIntsWritable getOutlinks() {
		return mOutlinks;
	}

	public void setOutlinks(ArrayListOfIntsWritable l) {
		mOutlinks = l;
	}

	public int getType() {
		return mType;
	}

	public void setType(int type) {
		if (type != TYPE_HUB_COMPLETE && type != TYPE_HUB_MASS
				&& type != TYPE_HUB_STRUCTURE && type != TYPE_AUTH_COMPLETE
				&& type != TYPE_AUTH_MASS && type != TYPE_AUTH_STRUCTURE
				&& type != TYPE_NODE_MASS && type != TYPE_NODE_STRUCTURE
				&& type != TYPE_NODE_COMPLETE)
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

		mInlinks = new ArrayListOfIntsWritable();
		mOutlinks = new ArrayListOfIntsWritable();
		
		if (mType == TYPE_HUB_MASS || mType == TYPE_NODE_MASS) {
			mHRank = in.readFloat();
			return;
		}
		
		if (mType == TYPE_AUTH_MASS || mType == TYPE_NODE_MASS) {
			mARank = in.readFloat();
			return;
		}

		if (mType == TYPE_HUB_COMPLETE || mType == TYPE_NODE_COMPLETE) {
			mHRank = in.readFloat();
		}
		
		if (mType == TYPE_AUTH_COMPLETE || mType == TYPE_NODE_COMPLETE) {
			mARank = in.readFloat();
		}

		//if (mType == TYPE_HUB_STRUCTURE || mType == TYPE_NODE_STRUCTURE || mType == TYPE_NODE_COMPLETE)
		//{
			//mOutlinks.readFields(in);
			mInlinks.readFields(in);
		//}
		
		//is this right -- inlinks go with hub and outlinks go with auth??? no -- other way around
		//if (mType == TYPE_AUTH_STRUCTURE || mType == TYPE_NODE_STRUCTURE || mType == TYPE_NODE_COMPLETE)
		//{
			mOutlinks.readFields(in);
			//mInlinks.readFields(in);
		//}
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

		if (mType == TYPE_HUB_MASS || mType == TYPE_NODE_MASS) {
			out.writeFloat(mHRank);
			return;
		}
		
		if (mType == TYPE_AUTH_MASS || mType == TYPE_NODE_MASS) {
			out.writeFloat(mARank);
			return;
		}

		if (mType == TYPE_HUB_COMPLETE || mType == TYPE_NODE_COMPLETE) {
			out.writeFloat(mHRank);
		}
		
		if (mType == TYPE_AUTH_COMPLETE || mType == TYPE_NODE_COMPLETE) {
			out.writeFloat(mARank);
		}

		mInlinks.write(out);
		mOutlinks.write(out);
	}

	public String toString() {
		StringBuilder s = new StringBuilder();

		s.append("{");
		s.append(mNodeId);
		s.append(" ");
		if (mType == TYPE_HUB_COMPLETE || mType == TYPE_HUB_MASS
				|| mType == TYPE_HUB_STRUCTURE) {
			s.append("H");
		} 
		else if (mType == TYPE_AUTH_COMPLETE || mType == TYPE_AUTH_MASS
				|| mType == TYPE_AUTH_STRUCTURE) {
			s.append("A");
		} else if ( mType == TYPE_NODE_COMPLETE || mType == TYPE_NODE_MASS ||
				mType == TYPE_NODE_STRUCTURE ){
			s.append("N");
		} else {
			s.append("?");
		}
		
		if (mType == TYPE_HUB_COMPLETE || mType == TYPE_HUB_MASS || mType == TYPE_NODE_COMPLETE) {
			s.append(" H: " + mHRank);
		}
		if (mType == TYPE_AUTH_COMPLETE || mType == TYPE_AUTH_MASS || mType == TYPE_NODE_COMPLETE) {
			s.append(" A: " + mARank);
		}
		
		s.append(" ");

		if (mOutlinks == null) {
			s.append("Out: {}");
		} else {
			s.append("Out: {");
			for (int i = 0; i < mOutlinks.size(); i++) {
				s.append(mOutlinks.get(i));
				if (i < mOutlinks.size() - 1)
					s.append(", ");
			}
			s.append("}");
		}
		
		s.append(" ");

		if (mInlinks == null) {
			s.append("In: {}");
		} else {
			s.append("In: {");
			for (int i = 0; i < mInlinks.size(); i++) {
				s.append(mInlinks.get(i));
				if (i < mInlinks.size() - 1)
					s.append(", ");
			}
			s.append("} ");
		}

		s.append("}");
		return s.toString();
	}

}
