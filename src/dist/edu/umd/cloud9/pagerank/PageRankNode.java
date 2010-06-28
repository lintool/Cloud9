/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank.
 * 
 * @author Jimmy Lin
 * @author Michael Schatz
 *
 */
public class PageRankNode implements Writable {

	public static final int TYPE_COMPLETE = 1;
	public static final int TYPE_MASS = 2;
	public static final int TYPE_STRUCTURE = 3;

	private int mType;
	private int mNodeId;
	private float mPageRank;
	private ArrayListOfIntsWritable mAdjacenyList;

	public PageRankNode() {
	}

	public float getPageRank() {
		return mPageRank;
	}

	public void setPageRank(float p) {
		mPageRank = p;
	}

	public int getNodeId() {
		return mNodeId;
	}

	public void setNodeId(int n) {
		mNodeId = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return mAdjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable l) {
		mAdjacenyList = l;
	}

	public int getType() {
		return mType;
	}

	public void setType(int type) {
		if (type != TYPE_COMPLETE && type != TYPE_MASS && type != TYPE_STRUCTURE)
			return;

		mType = type;
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in
	 *            source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		mType = in.readByte();

		mNodeId = in.readInt();

		if (mType == TYPE_MASS) {
			mPageRank = in.readFloat();
			return;
		}

		if (mType == TYPE_COMPLETE) {
			mPageRank = in.readFloat();
		}

		mAdjacenyList = new ArrayListOfIntsWritable();
		mAdjacenyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out
	 *            where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte((byte) mType);
		out.writeInt(mNodeId);

		if (mType == TYPE_MASS) {
			out.writeFloat(mPageRank);
			return;
		}

		if (mType == TYPE_COMPLETE) {
			out.writeFloat(mPageRank);
		}

		mAdjacenyList.write(out);
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();

		s.append("{");
		s.append(mNodeId);

		s.append(" ");
		s.append(mPageRank);
		s.append(" ");

		if (mAdjacenyList == null) {
			s.append("{}");
		} else {
			s.append("{");
			for (int i = 0; i < mAdjacenyList.size(); i++) {
				s.append(mAdjacenyList.get(i));
				if (i < mAdjacenyList.size() - 1)
					s.append(", ");
			}
			s.append("}");
		}

		s.append(" }");

		return s.toString();
	}
}
