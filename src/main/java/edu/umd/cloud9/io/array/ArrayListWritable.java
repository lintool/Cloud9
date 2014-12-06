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

package edu.umd.cloud9.io.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**
 * <p>
 * Writable extension of a Java ArrayList. Elements in the list must be homogeneous and must
 * implement Hadoop's Writable interface.
 * </p>
 *
 * @param <E> type of list element
 *
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */

public class ArrayListWritable<E extends Writable> extends ArrayList<E> implements Writable {
  private static final long serialVersionUID = 4911321393319821791L;

  /**
	 * Creates an ArrayListWritable object.
	 */
	public ArrayListWritable() {
		super();
	}

	/**
	 * Creates an ArrayListWritable object from an ArrayList.
	 */
	public ArrayListWritable(ArrayList<E> array) {
		super(array);
	}

	/**
	 * Deserializes the array.
	 *
	 * @param in source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numFields = in.readInt();
		if (numFields == 0)
			return;
		String className = in.readUTF();
		E obj;
		try {
			Class<E> c = (Class<E>) Class.forName(className);
			for (int i = 0; i < numFields; i++) {
				obj = (E) c.newInstance();
				obj.readFields(in);
				this.add(obj);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Serializes this array.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		if (size() == 0)
			return;
		E obj = get(0);

		out.writeUTF(obj.getClass().getCanonicalName());

		for (int i = 0; i < size(); i++) {
			obj = get(i);
			if (obj == null) {
				throw new IOException("Cannot serialize null fields!");
			}
			obj.write(out);
		}
	}

	/**
	 * Generates human-readable String representation of this ArrayList.
	 *
	 * @return human-readable String representation of this ArrayList
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < this.size(); i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(this.get(i));
		}
		sb.append("]");

		return sb.toString();
	}
}
