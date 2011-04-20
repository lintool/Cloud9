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

package edu.umd.cloud9.example.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank. 
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 *
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete(0),  // PageRank mass and adjacency list.
    Mass(1),      // PageRank mass only.
    Structure(2); // Adjacency list only.

    public int val;

    private Type(int v) {
      this.val = v;
    }
  };

	private Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeId;
	private float pagerank;
	private ArrayListOfIntsWritable adjacenyList;

	public PageRankNode() {}

	public float getPageRank() {
		return pagerank;
	}

	public void setPageRank(float p) {
		this.pagerank = p;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int n) {
		this.nodeId = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeId = in.readInt();

		if (type.equals(Type.Mass)) {
			pagerank = in.readFloat();
			return;
		}

		if (type.equals(Type.Complete)) {
			pagerank = in.readFloat();
		}

		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte((byte) type.val);
		out.writeInt(nodeId);

		if (type.equals(Type.Mass)) {
			out.writeFloat(pagerank);
			return;
		}

		if (type.equals(Type.Complete)) {
			out.writeFloat(pagerank);
		}

		adjacenyList.write(out);
	}

	@Override
	public String toString() {
		return String.format("{%d %.4f %s}",
				nodeId, pagerank, (adjacenyList == null ? "[]" : adjacenyList.toString(10)));
	}
}
