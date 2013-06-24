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

package edu.umd.cloud9.example.bfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for parallel breadth-first search.
 *
 * @author Jimmy Lin
 */
public class BfsNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // Complete structure.
    Distance((byte) 1),  // Distance only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

  private static final Type[] mapping = new Type[] { Type.Complete, Type.Distance, Type.Structure };

	private Type type;
	private int nodeid;
	private int distance;
	private ArrayListOfIntsWritable adjacenyList;

	public BfsNode() {}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int d) {
		distance = d;
	}

	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		nodeid = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable l) {
		adjacenyList = l;
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
		type = mapping[in.readByte()];

		nodeid = in.readInt();

		if (type.equals(Type.Distance)) {
			distance = in.readInt();
			return;
		}

		if (type.equals(Type.Complete)) {
			distance = in.readInt();
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
		out.writeByte(type.val);
		out.writeInt(nodeid);

    if (type.equals(Type.Distance)) {
      out.writeInt(distance);
      return;
    }

    if (type.equals(Type.Complete)) {
      out.writeInt(distance);
    }

		adjacenyList.write(out);
	}

	@Override
	public String toString() {
	   return String.format("{%d %d %s}",
	        nodeid, distance, (adjacenyList == null ? "[]" : adjacenyList.toString(10)));
	}

	/**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static BfsNode create(DataInput in) throws IOException {
    BfsNode m = new BfsNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static BfsNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
